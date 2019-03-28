using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SimpleStorage
{
    /// <summary>
    /// It is similiar to a table in a rdms database.
    /// </summary>
    public class Collection : IDisposable
    {
        //used for serializing

        private MemoryMappedFile mm_index = null;
        private MemoryMappedFile mm_data = null;
        private MemoryMappedFile mm_alloc = null;

        [StructLayout(LayoutKind.Sequential)]
        public struct DirectoryEntry
        {
            public ulong Key;
            public long CreationTime;
            public uint FirstSector;
            public uint Lengh;
        }


        [StructLayout(LayoutKind.Sequential)]
        private struct _CollectionHeader
        {
            public byte magic1;
            public byte magic2;
            public byte magic3;
            public byte magic4;

            public uint fill;
            public uint used;
            public uint mask;
        }

        

        [StructLayout(LayoutKind.Sequential)]
        private struct _AllocHeader
        {
            public int used;
            public int cap;
        }

        private readonly int _sizeof_col_hdr = Marshal.SizeOf(typeof(_CollectionHeader));
        private readonly int _sizeof_alloc_hdr = Marshal.SizeOf(typeof(_AllocHeader));
        private readonly int _sizeof_dir_entry = Marshal.SizeOf(typeof(DirectoryEntry));

        private string index_path;
        private string data_path;
        private string alloc_path;

        public Collection(string db_root, string collection_name)
        {
            index_path = Path.Combine(db_root, collection_name + ".index");
            data_path = Path.Combine(db_root, collection_name + ".data");
            alloc_path = Path.Combine(db_root, collection_name + ".alloc");

            
            try
            {
                // open existing collection
                mm_index = MemoryMappedFile.CreateFromFile(index_path);
                mm_data = MemoryMappedFile.CreateFromFile(data_path);
                mm_alloc = MemoryMappedFile.CreateFromFile(alloc_path);
            }
            catch (FileNotFoundException)
            {
                // create new collection
                mm_index = MemoryMappedFile.CreateFromFile(index_path, FileMode.CreateNew, null, 208);
                using (var acc = mm_index.CreateViewAccessor())
                {
                    var hdr = new _CollectionHeader();
                    byte[] magic = Encoding.ASCII.GetBytes("STCL");
                    hdr.magic1 = magic[0];
                    hdr.magic2 = magic[1];
                    hdr.magic3 = magic[2];
                    hdr.magic4 = magic[3];
                    hdr.used = 0;
                    hdr.fill = 0;
                    hdr.mask = 7;

                    acc.Write<_CollectionHeader>(0, ref hdr);

                    var empty_entry_data = CreateDirectoryEntry();
                    for(var i = 0; i < 8; i++)
                    {
                        acc.Write<DirectoryEntry>(_sizeof_col_hdr + i * _sizeof_dir_entry, ref empty_entry_data);
                    }
                }

                mm_data = MemoryMappedFile.CreateFromFile(data_path, FileMode.CreateNew, null, 256 * 8);
                mm_alloc = MemoryMappedFile.CreateFromFile(alloc_path, FileMode.CreateNew, null, 40);

                using (var acc = mm_alloc.CreateViewAccessor())
                {
                    var hdr = new _AllocHeader();

                    hdr.used = 1; // first segment is always used as a NULL segment
                    hdr.cap = 8;

                    acc.Write<_AllocHeader>(0, ref hdr);
                    int pos = _sizeof_alloc_hdr;
                    var _sizeof_int = Marshal.SizeOf(typeof(uint));
                    for(var i = 0; i < hdr.cap; i++)
                    {
                        acc.Write(pos, (uint)0);
                        pos += _sizeof_int;
                    }
                }
            }


        }

        public DirectoryEntry CreateDirectoryEntry()
        {
            DirectoryEntry e = new DirectoryEntry()
            {
                Key = 0,
                CreationTime = DateTime.UtcNow.Ticks,
                FirstSector = 0,
                Lengh = 0
            };

            return e;
        }
        private _CollectionHeader ReadHeader()
        {
            _CollectionHeader hdr;

            using (var index_acc = mm_index.CreateViewAccessor())
            {
                index_acc.Read<_CollectionHeader>(0, out hdr);
            }

            return hdr;
        }


        private byte[] RetrieveData(uint first_sector, int length)
        {
            var data = new byte[length];
            int bytes_read = 0;

            using (var acc_alloc = mm_alloc.CreateViewAccessor())
            using (var acc_data = mm_data.CreateViewAccessor())
            {
                while(length > 0)
                {
                    int bytes_to_read = length > 256 ? 256 : length;
                    acc_data.ReadArray<byte>(first_sector * 256, data, bytes_read, bytes_to_read);

                    bytes_read += bytes_to_read;
                    length -= bytes_to_read;

                    first_sector = acc_alloc.ReadUInt32(_sizeof_alloc_hdr + first_sector * 4);
                }
            }

            return data;
        }
        private uint StoreData(byte[] data)
        {
            // 1 - split data in 256 byte chunks
            int q = data.Length / 256;
            int r = data.Length % 256;
            int num_chunks = q + (r > 0 ? 1 : 0);

            List<byte[]> chunks = new List<byte[]>();
            for(int i = 0; i < q; i++)
            {
                byte[] chunk = new byte[256];
                Array.Copy(data, i * 256, chunk, 0, 256);
                chunks.Add(chunk);
            }

            if(r > 0)
            {
                byte[] chunk = new byte[r];
                Array.Copy(data, q * 256, chunk, 0, r);
                chunks.Add(chunk);
            }

            int newsize = 0;
            _AllocHeader hdr;
            using (var acc = mm_alloc.CreateViewAccessor())
            {
                byte[] hdr_data = new byte[_sizeof_alloc_hdr];
                acc.Read<_AllocHeader>(0, out hdr);
                newsize = hdr.used + chunks.Count;
            }

            int newcap = hdr.cap;
            // grow if necessary
            if(newsize > hdr.cap)
            {
                while (newcap < newsize)
                    newcap = 2 * newcap;

                mm_alloc.Dispose();
                mm_data.Dispose();
                mm_data = MemoryMappedFile.CreateFromFile(data_path, FileMode.Open, null, 256 * newcap);
                mm_alloc = MemoryMappedFile.CreateFromFile(alloc_path, FileMode.Open, null, _sizeof_alloc_hdr + newcap * 4);

                using (var mm_alloc_acc = mm_alloc.CreateViewAccessor())
                {
                    uint val = 0;
                    for(var i = hdr.cap; i < newcap; i++)
                    {
                        int pos = _sizeof_alloc_hdr + (i * 4);
                        mm_alloc_acc.Write<uint>(pos, ref val);
                    }


                }

            }

            using (var mm_alloc_acc = mm_alloc.CreateViewAccessor())
            {
                hdr.cap = newcap;
                hdr.used = newsize;
                mm_alloc_acc.Write<_AllocHeader>(0, ref hdr);
            }

            uint first = 0;
            uint prev = 0;
            uint last = 0;

            using (var alloc_acc = mm_alloc.CreateViewAccessor())
            using (var data_acc = mm_data.CreateViewAccessor())
            {
                foreach (var chunk in chunks)
                {
                    while(true)
                    {
                        last++;
                        var pos = _sizeof_alloc_hdr + (last * 4);
                        uint val = alloc_acc.ReadUInt32(pos);
                        if(val == 0) // empty slot
                        {
                            if (first == 0)
                            {
                                first = last;
                            } else
                            {
                                var pos2 = _sizeof_alloc_hdr + (prev * 4);
                                alloc_acc.Write(pos2, last);
                            }
                            prev = last;
                            data_acc.WriteArray(last * 256, chunk, 0, chunk.Length);
                            break;
                        }

                    }
                }

                uint val2 = uint.MaxValue;
                var ending_pos = _sizeof_alloc_hdr + (prev * 4);
                alloc_acc.Write(ending_pos, val2);

            }


            return first;
        }

        private long GetKeySlot(_CollectionHeader hdr, ulong key, out DirectoryEntry e)
        {
            using (var acc = mm_index.CreateViewAccessor())
            {
                for (long _i = hdr.mask & (uint)key; ; _i = (5 * _i + 1) & hdr.mask)
                {
                    long pos = _sizeof_col_hdr + _i * _sizeof_dir_entry;
                    acc.Read<DirectoryEntry>(pos, out e);
                    

                    // empty entry we can store to this address
                    if (e.FirstSector == 0 || e.Key == key)
                    {
                        return pos;
                    }

                }
            }

        }
        /// <summary>
        /// store a key in collection
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="data">value</param>
        /// <returns></returns>
        public bool Put(ulong key, byte[] data)
        {
            // - Find an empty slot
            var hdr = ReadHeader();

            DirectoryEntry e;
            var pos = GetKeySlot(hdr, key, out e);
            if(e.Key != 0)
                throw new ArgumentException("Slot is alredy used");


            using (var acc = mm_index.CreateViewAccessor())
            {
                e = new DirectoryEntry()
                {
                    Key = key,
                    CreationTime = DateTime.UtcNow.Ticks,
                    FirstSector = StoreData(data),
                    Lengh = (uint)data.Length
                };

                acc.Write<DirectoryEntry>(pos, ref e);
            }

            return true;
        }

        public byte[] Get(ulong key)
        {
            var hdr = ReadHeader();

            DirectoryEntry e;
            var pos = GetKeySlot(hdr, key, out e);
            if (e.Key == 0)
                throw new ArgumentException("Not Found");

            return RetrieveData(e.FirstSector, (int)e.Lengh);
        }

        public void Dispose()
        {
            mm_index.Dispose();
            mm_index = null;

            mm_data.Dispose();
            mm_data = null;

            mm_alloc.Dispose();
            mm_data = null;
        }
    }
}
