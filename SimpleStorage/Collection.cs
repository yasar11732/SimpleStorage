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
            public uint hash;
            // if keySector is zero, this slot is not actively used
            public uint keySector;
            public long cTime;
            // if dataSector is zero, this slot was never used
            public uint dataSector;
            public uint length;
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
            public uint used;
            public uint cap;
        }

        private readonly int __sizeof_col_hdr = Marshal.SizeOf(typeof(_CollectionHeader));
        private readonly int __sizeof_alloc_hdr = Marshal.SizeOf(typeof(_AllocHeader));
        private readonly int __sizeof_dir_entry = Marshal.SizeOf(typeof(DirectoryEntry));
        private readonly int __sizeof_uint = Marshal.SizeOf(typeof(uint));

        private uint DirEntryPosFromIndex(uint index)
        {
            return Convert.ToUInt32(__sizeof_col_hdr + index * __sizeof_dir_entry);
        }

        private uint AllocPosFromIndex(uint index)
        {
            return Convert.ToUInt32(__sizeof_alloc_hdr + index * __sizeof_uint);

        }
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
                    for(uint i = 0; i < 8; i++)
                    {
                        acc.Write<DirectoryEntry>(DirEntryPosFromIndex(i), ref empty_entry_data);
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
                    var _sizeof_int = Marshal.SizeOf(typeof(uint));
                    for(uint i = 0; i < hdr.cap; i++)
                    {
                        acc.Write(AllocPosFromIndex(i), (uint)0);
                    }
                }
            }


        }

        public void Remove(string key)
        {
            // - Find an empty slot
            var hdr = ReadHeader();

            byte[] key_slice = new byte[256];
            uint hash = hashAndTruncate(key, ref key_slice);

            DirectoryEntry e;
            var pos = GetKeySlot(hdr, hash, key_slice, out e);
            if (e.keySector == 0) // key doesn't exist, nothing to remove
                return;

            
            RemoveData(e.keySector);
            e.keySector = 0;
            RemoveData(e.dataSector);
            using (var acc = mm_index.CreateViewAccessor())
            {
                acc.Write<DirectoryEntry>(pos, ref e);
                hdr.used -= 1;
                acc.Write<_CollectionHeader>(0, ref hdr);
            }

        }

        public DirectoryEntry CreateDirectoryEntry()
        {
            DirectoryEntry e = new DirectoryEntry()
            {
                hash = 0,
                keySector = 0,
                cTime = DateTime.UtcNow.Ticks,
                dataSector = 0,
                length = 0
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

        private void RemoveData(uint first_sector)
        {
            uint next_sector = 0;
            _AllocHeader hdr;
            using (var acc = mm_alloc.CreateViewAccessor())
            {
                acc.Read<_AllocHeader>(0, out hdr);
                while (first_sector != uint.MaxValue)
                {
                    var pos = AllocPosFromIndex(first_sector);
                    next_sector = acc.ReadUInt32(pos);
                    acc.Write(pos, (uint)0);
                    hdr.used -= 1;
                    first_sector = next_sector;
                }
                acc.Write<_AllocHeader>(0, ref hdr);
            }

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

                    first_sector = acc_alloc.ReadUInt32(AllocPosFromIndex(first_sector));
                }
            }

            return data;
        }

        private _AllocHeader PrepareSpace(uint num_chunks)
        {
            _AllocHeader hdr;
            uint target_cap;
            using (var acc = mm_alloc.CreateViewAccessor())
            {
                acc.Read<_AllocHeader>(0, out hdr);
                target_cap = hdr.cap;
                while (target_cap < hdr.used + num_chunks)
                    target_cap = target_cap * 2;


            }

            hdr.used = hdr.used + num_chunks;

            if (target_cap > hdr.cap)
            {
                mm_alloc.Dispose();
                mm_data.Dispose();
                mm_data = MemoryMappedFile.CreateFromFile(data_path, FileMode.Open, null, 256 * target_cap);
                mm_alloc = MemoryMappedFile.CreateFromFile(alloc_path, FileMode.Open, null, AllocPosFromIndex(target_cap));
                using (var mm_alloc_acc = mm_alloc.CreateViewAccessor())
                {
                    uint val = 0;
                    for (uint i = hdr.cap; i < target_cap; i++)
                    {
                        mm_alloc_acc.Write(AllocPosFromIndex(i),val);
                    }
                    hdr.cap = target_cap;
                }
            }

            using (var mm_alloc_acc = mm_alloc.CreateViewAccessor())
            {
                mm_alloc_acc.Write<_AllocHeader>(0, ref hdr);
            }

            return hdr;
        }
        private uint StoreData(byte[] data)
        {
            // 1 - split data in 256 byte chunks
            var num_chunks = data.Length / 256;
            if (num_chunks * 256 < data.Length)
                num_chunks++;

            PrepareSpace((uint)num_chunks);

            uint first = 0;
            uint prev = 0;
            uint last = 0;
            int bytes_rem = data.Length;

            using (var alloc_acc = mm_alloc.CreateViewAccessor())
            using (var data_acc = mm_data.CreateViewAccessor())
            {
                while(bytes_rem > 0)
                {
                    int bytes_to_write = bytes_rem >= 256 ? 256 : bytes_rem;
                    while(true)
                    {
                        last++;
                        var pos = AllocPosFromIndex(last);
                        uint val = alloc_acc.ReadUInt32(pos);
                        if(val == 0) // empty slot
                        {
                            if (first == 0)
                            {
                                first = last;
                            } else
                            {
                                var pos2 = AllocPosFromIndex(prev);
                                alloc_acc.Write(pos2, last);
                            }
                            prev = last;
                            data_acc.WriteArray(last * 256, data, (data.Length - bytes_rem), bytes_to_write);
                            bytes_rem -= bytes_to_write;
                            break;
                        }

                    }
                }

                alloc_acc.Write(AllocPosFromIndex(prev), uint.MaxValue);

            }


            return first;
        }

        private void DictGrow(ref _CollectionHeader hdr)
        {
            var orig_mask = hdr.mask;
            hdr.mask = (2 * hdr.mask) + 1;
            hdr.fill = hdr.used;

            var next_size = DirEntryPosFromIndex(hdr.mask + 1);

            var mm_index_next = MemoryMappedFile.CreateFromFile(index_path + ".new", FileMode.CreateNew, null, next_size);
            using(var acc = mm_index_next.CreateViewAccessor())
            using(var acc_old = mm_index.CreateViewAccessor())
            {
                // -- write new header
                acc.Write<_CollectionHeader>(0, ref hdr);

                // initialize empty slots
                var entry = CreateDirectoryEntry();
                var remaining = hdr.mask + 1;
                for(uint i = 0; i < remaining; i++)
                {
                    acc.Write<DirectoryEntry>(DirEntryPosFromIndex(i), ref entry);
                }

                // copy slots from old index to new
                remaining = orig_mask + 1;
                for (uint i = 0; i < remaining; i++)
                {
                    acc_old.Read<DirectoryEntry>(DirEntryPosFromIndex(i), out entry);
                    if(entry.keySector != 0)
                    {
                        DirectoryEntry slot;
                        for(uint j = entry.hash&hdr.mask; ; j = (5*j+1)&hdr.mask)
                        {
                            var _pos = DirEntryPosFromIndex(j);
                            acc.Read<DirectoryEntry>(_pos, out slot);
                            if(slot.keySector == 0)
                            {
                                acc.Write<DirectoryEntry>(_pos, ref entry);
                                break;
                            }
                        }
                    }
                }

            }

            mm_index.Dispose();
            mm_index_next.Dispose();

            File.Delete(index_path + ".bk");
            File.Move(index_path, index_path + ".bk");
            File.Move(index_path + ".new", index_path);
            mm_index = MemoryMappedFile.CreateFromFile(index_path, FileMode.Open);

        }
        private uint GetKeySlot(_CollectionHeader hdr, uint hash, byte[] key_slice, out DirectoryEntry e)
        {
            // key is truncated to 256 bytes when stored


            using (var acc = mm_index.CreateViewAccessor())
            {
                for (uint _i = hash&hdr.mask; ; _i = (5 * _i + 1) & hdr.mask)
                {
                    var pos = DirEntryPosFromIndex(_i);
                    acc.Read<DirectoryEntry>(pos, out e);
                    

                    // search ends when we hit a slot that was never used
                    if (e.dataSector == 0)
                    {
                        return pos;
                    }

                    // we have found a match
                    if(e.keySector != 0
                       && e.hash == hash
                       && key_slice.SequenceEqual(RetrieveData(e.keySector, 256)))
                    {
                        return pos;
                    }

                }
            }

        }

        private uint hashAndTruncate(string key, ref byte[] key_slice)
        {
            Encoding.UTF8.GetBytes(key).Take(256).ToArray().CopyTo(key_slice, 0);
            return FNV64.Compute(Encoding.UTF8.GetBytes(key));
        }
        /// <summary>
        /// store a key in collection
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="data">value</param>
        /// <returns></returns>
        public bool Put(string key, byte[] data)
        {
            byte[] key_slice = new byte[256];
            uint hash = hashAndTruncate(key, ref key_slice);

            var hdr = ReadHeader();
            if ((double)hdr.fill / (double)hdr.mask > 0.65)
            {
                DictGrow(ref hdr);
            }

            DirectoryEntry e;
            var pos = GetKeySlot(hdr, hash, key_slice, out e);
            if(e.dataSector != 0)
                throw new ArgumentException("Slot is alredy used");
            
            

            using (var acc = mm_index.CreateViewAccessor())
            {
                e = new DirectoryEntry()
                {
                    hash = hash,
                    keySector = StoreData(key_slice),
                    cTime = DateTime.UtcNow.Ticks,
                    dataSector = StoreData(data),
                    length = (uint)data.Length
                };

                acc.Write<DirectoryEntry>(pos, ref e);
                hdr.fill += 1;
                hdr.used += 1;
                acc.Write<_CollectionHeader>(0, ref hdr);
            }

            return true;
        }

        public byte[] Get(string key)
        {
            byte[] key_slice = new byte[256];
            uint hash = hashAndTruncate(key, ref key_slice);

            var hdr = ReadHeader();

            DirectoryEntry e;
            var pos = GetKeySlot(hdr, hash, key_slice, out e);
            if (e.dataSector == 0)
                throw new ArgumentException("Not Found");

            return RetrieveData(e.dataSector, (int)e.length);
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
