using System;
using System.Collections.Generic;
using System.IO;
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
        private class Index
        {
            private byte[] _rawData;
            private FileStream f;

            private enum FieldOffset
            {
                MAGIC = 0,
                USED  = 4,
                FILL = 8,
                MASK = 12
            }

            private enum FieldOffsetEntry
            {
                HASH = 0,
                KEYSECTOR = 4,
                DATASECTOR = 8,
                CTIME = 12
            }

            private readonly byte _sizeof_header = 16;
            private readonly byte _sizeof_entry = 20;

            public uint used
            {
                get
                {
                    return BitConverter.ToUInt32(_rawData, (int)FieldOffset.USED);
                }
                set
                {
                    BitConverter.GetBytes(value).CopyTo(_rawData, (int)FieldOffset.USED);
                }
            }

            public uint fill
            {
                get
                {
                    return BitConverter.ToUInt32(_rawData, (int)FieldOffset.FILL);
                }
                set
                {
                    BitConverter.GetBytes(value).CopyTo(_rawData, (int)FieldOffset.FILL);
                }
            }

            public uint mask
            {
                get
                {
                    return BitConverter.ToUInt32(_rawData, (int)FieldOffset.MASK);
                }
                set
                {
                    BitConverter.GetBytes(value).CopyTo(_rawData, (int)FieldOffset.MASK);
                }
            }

            public byte[] rawData
            {
                get
                {
                    return _rawData;
                }
            }

            public Index(string index_path)
            {
                byte[] magic = Encoding.ASCII.GetBytes("STCL");
                f = File.Open(index_path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
                if(f.Length == 0)
                {
                    // this file is created newly, initialize it
                    _rawData = new byte[_sizeof_header + 8 * _sizeof_entry];
                    magic.CopyTo(_rawData, 0);
                    used = 0;
                    fill = 0;
                    mask = 7;
                } else
                {
                    // check file type is correct
                    _rawData = new byte[f.Length];
                    f.Read(_rawData, 0, (int)f.Length);
                    if(!_rawData.Take(4).SequenceEqual(magic))
                    {
                        throw new InvalidDataException();
                    }
                }
            }

            /// <summary>
            /// Writes changes to underlying FileStream
            /// </summary>
            public void Flush()
            {
                f.Seek(0, SeekOrigin.Begin);
                f.Write(_rawData, 0, _rawData.Length);
                f.Flush();
            }

            private int SlotPosition(int slot)
            {
                return _sizeof_header + slot * _sizeof_entry;
            }



            public uint GetHash(int index)
            {
                return BitConverter
                    .ToUInt32(_rawData, SlotPosition(index) + (int)FieldOffsetEntry.HASH);
            }

            public void SetHash(int index, uint hash)
            {
                BitConverter
                    .GetBytes(hash)
                    .CopyTo(_rawData, SlotPosition(index) + (int)FieldOffsetEntry.HASH);
            }

            public uint GetKeySector(int index)
            {
                return BitConverter
                    .ToUInt32(_rawData, SlotPosition(index) + (int)FieldOffsetEntry.KEYSECTOR);
            }

            public void SetKeySector(int index, uint keysector)
            {
                BitConverter
                    .GetBytes(keysector)
                    .CopyTo(_rawData, SlotPosition(index) + (int)FieldOffsetEntry.KEYSECTOR);
            }

            public uint GetDataSector(int index)
            {
                return BitConverter
                    .ToUInt32(_rawData, SlotPosition(index) + (int)FieldOffsetEntry.DATASECTOR);
            }

            public void SetDataSector(int index, uint datasector)
            {
                BitConverter
                    .GetBytes(datasector)
                    .CopyTo(_rawData, SlotPosition(index) + (int)FieldOffsetEntry.DATASECTOR);
            }

            public Int64 GetDataCreateTime(int index)
            {
                return BitConverter
                    .ToInt64(_rawData, SlotPosition(index) + (int)FieldOffsetEntry.CTIME);
            }

            public void SetDataCreateTime(int index, Int64 createTime)
            {
                BitConverter
                    .GetBytes(createTime)
                    .CopyTo(_rawData, SlotPosition(index) + (int)FieldOffsetEntry.CTIME);
            }

            public void Grow()
            {
                var old_mask = mask;
                var next_mask = (2 * old_mask) + 1;

                var _rawDataOld = _rawData;
                _rawData = new byte[_sizeof_header + (next_mask + 1) * _sizeof_entry];
                Array.Copy(_rawDataOld, _rawData, _sizeof_header);
                mask = next_mask;
                fill = used;

                for (int i = 0; i <= old_mask; i++)
                {
                    var slot_pos = SlotPosition(i);
                    var key_sector_at_i = BitConverter
                    .ToUInt32(_rawDataOld, slot_pos + (int)FieldOffsetEntry.KEYSECTOR);

                    if (key_sector_at_i != 0)
                    {
                        var hash_at_i = BitConverter
                            .ToUInt32(_rawDataOld, slot_pos + (int)FieldOffsetEntry.HASH);

                        for (uint j = hash_at_i & next_mask; ; j = (5 * j + 1) & next_mask)
                        {
                            if (GetKeySector((int)j) == 0)
                            {
                                Array.Copy(_rawDataOld,
                                    SlotPosition(i),
                                    _rawData,
                                    SlotPosition((int)j),
                                    _sizeof_entry
                                    );
                                break;
                            }
                        }

                    }
                }

            }

            public void Dispose()
            {
                Flush();
                f.Dispose();
            }
        }

        private class Storage
        {
            private byte[] _rawData;
            private readonly byte[] _zero = new byte[] { 0, 0, 0, 0 };
            private readonly byte[] _magic = new byte[] { 83, 84, 65, 76 }; // STAL
            private FileStream f;
            private FileStream d;
            private uint brk;

            /*
             * Binary Format of Allocation Table
             * First 4 byte is interpreted as 32-bit unsigned integer shows current length of data file
             * Second 4 byte is "STAL" to check for file type
             * an array of free list entries follows;
             * struct entry {
             *    void * data (Uint32)
             *    size_t size (Uint32)
             * }
             */

            public Storage(string alloc_path, string data_path)
            {
                d = File.Open(data_path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);

                f = File.Open(alloc_path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
                // created newly
                if (f.Length == 0)
                {
                    _rawData = new byte[16];
                    _magic.CopyTo(_rawData, 4);
                    
                } else
                {
                    _rawData = new byte[f.Length];
                    f.Read(_rawData, 0, (int)f.Length);
                    var _magic_check = new byte[] { _rawData[4], _rawData[5], _rawData[6], _rawData[7] };
                    if (!_magic.SequenceEqual(_magic_check))
                        throw new InvalidDataException("Allocation table is corrupted");
                }

                brk = BitConverter.ToUInt32(_rawData, 0);

                // never return 0 from alloc, because it is used as null
                // align it to 8 bytes, because of reasons
                if (brk == 0)
                    brk = 8;

            }

            // write changes to underlying FileStream
            // this does not guarantee that changes reach the disk immediately
            public void Flush()
            {
                d.Flush();
                f.Seek(0, SeekOrigin.Begin);
                BitConverter.GetBytes(brk).CopyTo(_rawData, 0);
                f.Write(_rawData, 0, _rawData.Length);
                f.Flush();
            }

            public void Dispose()
            {
                Flush();
                f.Dispose();
                d.Dispose();
            }

            // round a number up to nearest power of 2
            private uint RoundUp(uint size)
            {
                if (size == 0)
                    return 0;
                uint p = 1;
                while (p < size)
                    p *= 2;
                return p;
            }

            private uint Alloc(uint size)
            {
                size = RoundUp(size);
                for(int i = 8; ; i+=8)
                {
                    uint data = BitConverter.ToUInt32(_rawData, i);
                    uint length = BitConverter.ToUInt32(_rawData, i + 4);

                    if (data == 0 && length == 0)
                        break; // and of search, no free slots

                    if (data == 0)
                        continue;

                    if(length == size)
                    {
                        // remove this node from free queue, and return
                        _zero.CopyTo(_rawData, i);
                        return data;
                    }

                }

                // no hit from free list, allocate new space
                var v = brk;

                // We can't address more than 4GB of memory using uint32
                if (UInt32.MaxValue - size < brk)
                    throw new Exception("Cannot allocate more space");

                brk += size;
                return v;
            }

            private void Free(uint data, uint length)
            {
                length = RoundUp(length);
                int last_slot = _rawData.Length / 8;

                for (int i = 8; ; i+=8)
                {
                    uint _data = BitConverter.ToUInt32(_rawData, i);
                    if(_data == 0)
                    {
                        BitConverter.GetBytes(data).CopyTo(_rawData, i);
                        BitConverter.GetBytes(length).CopyTo(_rawData, i + 4);
                        if (i+8 >= _rawData.Length)
                            Array.Resize(ref _rawData, 2 * _rawData.Length);
                        return;
                    }
                }
            }

            public uint Store(byte[] data)
            {
                uint pos = Alloc((uint)data.Length + 4);
                d.Seek(pos, SeekOrigin.Begin);
                d.Write(BitConverter.GetBytes((uint)data.Length), 0, 4);
                d.Write(data, 0, data.Length);
                Flush();
                return pos;
            }

            public void Delete(uint pos)
            {
                byte[] buffer = new byte[4];
                d.Seek(pos, SeekOrigin.Begin);
                d.Read(buffer, 0, 4);
                uint len = BitConverter.ToUInt32(buffer, 0);
                Free(pos, len + 4);
                Flush();
            }

            public byte[] Retrieve(uint pos)
            {
                byte[] buffer = new byte[4];
                d.Seek(pos, SeekOrigin.Begin);
                d.Read(buffer, 0, 4);
                uint len = BitConverter.ToUInt32(buffer, 0);
                buffer = new byte[len];
                d.Read(buffer, 0, (int)len);
                return buffer;
            }

            
        }

        private Index _index;
        private Storage _storage;

        public Collection(string db_root, string collection_name)
        {
            _index = new Index(Path.Combine(db_root, collection_name + ".index"));
            _storage = new Storage(Path.Combine(db_root, collection_name + ".alloc"), Path.Combine(db_root, collection_name + ".data"));


        }

        /// <summary>
        /// Removes all entries created before a certain time
        /// </summary>
        /// <param name="ticks"></param>
        public void RemoveOlderThan(TimeSpan s)
        {
            var ticks = DateTime.UtcNow.Subtract(s).Ticks;

            uint num_slots = _index.mask;

            for (int i = 0; i <= num_slots; i++)
            {
                var key_sector = _index.GetKeySector(i);

                if (key_sector != 0 && _index.GetDataCreateTime(i) < ticks)
                {
                    _storage.Delete(key_sector);
                    _index.SetKeySector(i, 0);
                    _storage.Delete(_index.GetDataSector(i));
                    _index.used -= 1;
                }
            }

            _index.Flush();
        }


        public void Remove(string key)
        {

            var keybytes = Encoding.UTF8.GetBytes(key);
            uint hash = FNV64.Compute(keybytes);

            uint pos;
            if (!GetKeySlot(hash, keybytes, out pos))
                return;

            int _pos = (int)pos;

            _storage.Delete(_index.GetKeySector(_pos));
            _index.SetKeySector(_pos, 0);
            _storage.Delete(_index.GetDataSector(_pos));
            _index.used -= 1;
            _index.Flush();
        }



        private bool GetKeySlot(uint hash, byte[] key, out uint pos)
        {
            // key is truncated to 256 bytes when stored

            var mask = _index.mask;

            for (pos = hash&mask; ; pos = (5 * pos + 1) & mask)
            {
                var datasector = _index.GetDataSector((int)pos);
                // search ends when we hit a slot that was never used
                if (datasector == 0)
                {
                    return false;
                }

                var keysector = _index.GetKeySector((int)pos);

                if (keysector != 0
                    && hash == _index.GetHash((int)pos)
                    && key.SequenceEqual(_storage.Retrieve(keysector)))
                    return true;

            }


        }

        /// <summary>
        /// store a key in collection
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="data">value</param>
        /// <returns></returns>
        public bool Put(string key, byte[] data)
        {
            var keybytes = Encoding.UTF8.GetBytes(key);
            uint hash = FNV64.Compute(keybytes);

            if ((double)_index.fill / (double)_index.mask > 0.65)
            {
                _index.Grow();
            }

            uint _pos;
            if(GetKeySlot(hash, keybytes, out _pos))
            {
                // update existing slot
                int pos = (int)_pos;
                _storage.Delete(_index.GetDataSector(pos));
                _index.SetDataCreateTime(pos, DateTime.UtcNow.Ticks);
                _index.SetDataSector(pos, _storage.Store(data));

            } else
            {
                int pos = (int)_pos;
                _index.SetHash(pos, hash);
                _index.SetKeySector(pos, _storage.Store(keybytes));
                _index.SetDataCreateTime(pos, DateTime.UtcNow.Ticks);
                _index.SetDataSector(pos, _storage.Store(data));

                _index.fill += 1;
                _index.used += 1;
            }

            _index.Flush();

            return true;
        }

        public byte[] Get(string key)
        {
            var keybytes = Encoding.UTF8.GetBytes(key);
            uint hash = FNV64.Compute(keybytes);

            uint _pos;

            if(!GetKeySlot(hash, keybytes, out _pos))
                return null;

            int pos = (int)_pos;

            byte[] result = _storage.Retrieve(_index.GetDataSector(pos));
            return result;
        }

        public byte[] GetOrExpire(string key, TimeSpan s)
        {
            var ticks = DateTime.UtcNow.Subtract(s).Ticks;
            
            var keybytes = Encoding.UTF8.GetBytes(key);
            uint hash = FNV64.Compute(keybytes);

            uint _pos;

            if (!GetKeySlot(hash, keybytes, out _pos))
                return null;
            var i = (int)_pos;

            if (_index.GetDataCreateTime(i) < ticks)
            {
                var key_sector = _index.GetKeySector(i);

                _storage.Delete(key_sector);
                _index.SetKeySector(i, 0);
                _storage.Delete(_index.GetDataSector(i));
                _index.used -= 1;
                _index.Flush();
                return null;
            } else
            {
                return _storage.Retrieve(_index.GetDataSector(i));
            }
        }

        public void Dispose()
        {
            _index.Dispose();
            _storage.Dispose();
        }
    }
}
