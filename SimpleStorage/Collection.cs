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
        private MemoryMappedFile mm_index = null;
        private MemoryMappedFile mm_data = null;
        private MemoryMappedFile mm_alloc = null;

        [StructLayout(LayoutKind.Sequential)]
        private struct _CollectionHeader
        {
            public char magic1;
            public char magic2;
            public char magic3;
            public char magic4;

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

        public Collection(string db_root, string collection_name)
        {
            var index_path = Path.Combine(db_root, collection_name + ".index");
            var data_path = Path.Combine(db_root, collection_name + ".data");
            var alloc_path = Path.Combine(db_root, collection_name + ".alloc");

            
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
                using (var acc = mm_index.CreateViewStream())
                {
                    var hdr = new _CollectionHeader();
                    hdr.magic1 = 'S';
                    hdr.magic2 = 'T';
                    hdr.magic3 = 'C';
                    hdr.magic4 = 'L';
                    hdr.used = 0;
                    hdr.fill = 0;
                    hdr.mask = 7;
                    byte[] hdr_data = StructTools.RawSerialize(hdr);

                    acc.Write(hdr_data, 0, hdr_data.Length);
                    var empty_entry_data = (new DirectoryEntry(0, 0, 0)).Serialize();
                    for(var i = 0; i < 8; i++)
                    {
                        acc.Write(empty_entry_data, 0, empty_entry_data.Length);
                    }
                }
                mm_data = MemoryMappedFile.CreateFromFile(data_path, FileMode.CreateNew, null, 256 * 8);
                mm_alloc = MemoryMappedFile.CreateFromFile(alloc_path, FileMode.CreateNew, null, 40);
                using (var acc = mm_alloc.CreateViewStream())
                {
                    var hdr = new _AllocHeader();

                    hdr.used = 0;
                    hdr.cap = 8;

                    byte[] hdr_data = StructTools.RawSerialize(hdr);
                    acc.Write(hdr_data, 0, hdr_data.Length);

                    for(int i = 0; i < 32; i++)
                    {
                        acc.WriteByte(0);
                    }
                }
            }


        }

        private _CollectionHeader ReadHeader()
        {
            byte[] hdr_bytes = new byte[_sizeof_col_hdr];

            using (var index_accessor = mm_index.CreateViewStream())
            {
                if (index_accessor.Read(hdr_bytes, 0, _sizeof_col_hdr) != _sizeof_col_hdr)
                {
                    throw new IOException();
                }
            }

            return StructTools.RawDeserialize<_CollectionHeader>(hdr_bytes, 0);
        }

        private uint StoreData(byte[] data)
        {
            return 5;
        }

        private bool GetKeySlot(_CollectionHeader hdr, ulong key, out MemoryMappedViewStream s)
        {

            for (uint _i = hdr.mask & (uint)key; ; _i = (5 * _i + 1) & (uint)key)
            {
                s = mm_index.CreateViewStream(_sizeof_col_hdr + (_i * DirectoryEntry.RawSize), DirectoryEntry.RawSize);
                var dir_entry_bytes = new byte[DirectoryEntry.RawSize];

                s.Read(dir_entry_bytes, 0, DirectoryEntry.RawSize);
                s.Seek(0, SeekOrigin.Begin);

                DirectoryEntry e = new DirectoryEntry(dir_entry_bytes, 0);

                // empty entry we can store to this address
                if (e.FirstSector == 0 )
                {
                    return false;
                }

                if(e.Key == key)
                {
                    return true;
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
            MemoryMappedViewStream s;
            if(GetKeySlot(hdr, key, out s))
            {
                throw new ArgumentException("Slot is alredy used");
            } else
            {
                // we got a free slot
                DirectoryEntry e = new DirectoryEntry(key, StoreData(data), (uint)data.Length);
                byte[] e_bytes = e.Serialize();
                s.Write(e_bytes, 0, e_bytes.Length);
                s.Dispose();
            }
            return true;
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
