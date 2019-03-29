using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleStorage
{
    public class FNV64
    {
        public static uint Compute(byte[] s)
        {
            uint hash = 2166136261;
            int array_size = s.Length;
            for(int i = 0; i < array_size; i++)
            {
                hash ^= s[i];
                hash *= 16777619;
            }

            return hash;
        }
    }
}
