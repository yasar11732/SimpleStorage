using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace SimpleStorage
{
    public class DirectoryEntry
    {
        //used for serializing
        [StructLayout(LayoutKind.Sequential)]
        private struct _DirectoryEntry
        {
            public ulong Key;
            public long CreationTime;
            public uint FirstSector;
            public uint Lengh;
        }

        private _DirectoryEntry _wrapped;

        /// <summary>
        /// 64 bit hash key
        /// </summary>
        public ulong Key
        {
            get { return _wrapped.Key; }
            set { _wrapped.Key = value; }
        }

        /// <summary>
        /// Gets the number of ticks that 
        /// </summary>
        public long CreationTime
        {
            get { return _wrapped.CreationTime; }
        }

        public uint FirstSector
        {
            get { return _wrapped.FirstSector; }
            set { _wrapped.FirstSector = value; }
        }

        public uint Lengh
        {
            get { return _wrapped.Lengh; }
            set { _wrapped.Lengh = value; }
        }

        public DirectoryEntry()
        {
            _wrapped = new _DirectoryEntry()
            {
                CreationTime = DateTime.UtcNow.Ticks
            };

        }

        public DirectoryEntry(ulong key, uint first_sector, uint length)
        {
            _wrapped = new _DirectoryEntry()
            {
                Key = key,
                CreationTime = DateTime.UtcNow.Ticks,
                FirstSector = first_sector,
                Lengh = length
            };

        }

        public DirectoryEntry(byte[] data, int position)
        {
            _wrapped = StructTools.RawDeserialize<_DirectoryEntry>(data, position);
        }

        public byte[] Serialize()
        {
            return StructTools.RawSerialize(_wrapped);
        }
    }
}
