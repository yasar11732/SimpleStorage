using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleStorage
{
    public class FNV64
    {
        public static ulong Compute(byte[] s)
        {
            ulong hash = 14695981039346656037;
            int array_size = s.Length;
            for(int i = 0; i < array_size; i++)
            {
                hash ^= s[i];
                hash *= 1099511628211;
            }

            return hash;
        }
    }
}
