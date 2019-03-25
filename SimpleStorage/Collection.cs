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
