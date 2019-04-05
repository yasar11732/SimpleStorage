using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace Tester
{
    public partial class Form1 : Form
    {
        private string src = null;
        private string dest = null;

        public Form1()
        {
            InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            var f = new FolderBrowserDialog();
            f.RootFolder = Environment.SpecialFolder.MyDocuments;
            if(f.ShowDialog() == DialogResult.OK)
            {
                src = f.SelectedPath;
                textBox1.Text = src;
            }
        }

        private void button2_Click(object sender, EventArgs e)
        {
            var f = new FolderBrowserDialog();
            f.RootFolder = Environment.SpecialFolder.MyDocuments;
            if (f.ShowDialog() == DialogResult.OK)
            {
                dest = f.SelectedPath;
                textBox2.Text = dest;
            }
        }

        private void button3_Click(object sender, EventArgs e)
        {
            string[] files = Directory.GetFiles(src);

            Dictionary<string, byte[]> files_to_put = new Dictionary<string, byte[]>();

            int totalbytes = 0;
            int totalfiles = 0;

            var sw1 = new Stopwatch();
            sw1.Start();
            foreach (string f in files)
            {
                files_to_put[f] = File.ReadAllBytes(f);
                totalbytes += files_to_put[f].Length;
                totalfiles++;
            }
            sw1.Stop();
            MessageBox.Show(String.Format("Dosyaları diskten okuma süresi: {0} milisaniye", sw1.ElapsedMilliseconds));

            using (var db = new SimpleStorage.Database(dest))
            {
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                foreach (var entry in files_to_put)
                {
                    db.Put("files", entry.Key, entry.Value);
                }
                stopwatch.Stop();

                MessageBox.Show(String.Format("{0} byte toplam boyutunda {1} dosya {2} milisaniyede eklendi.", totalbytes, totalfiles, stopwatch.ElapsedMilliseconds));
            }

        }

        private void button4_Click(object sender, EventArgs e)
        {
            string[] files = Directory.GetFiles(src);

            Dictionary<string, byte[]> files_to_put = new Dictionary<string, byte[]>();
            Dictionary<string, byte[]> results = new Dictionary<string, byte[]>();

            int totalbytes = 0;
            int totalfiles = 0;

            foreach (string f in files)
            {
                files_to_put[f] = File.ReadAllBytes(f);
                totalbytes += files_to_put[f].Length;
                totalfiles++;
            }

            using (var db = new SimpleStorage.Database(dest))
            {
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                foreach (var entry in files_to_put)
                {
                    results[entry.Key] = db.Get("files", entry.Key);
                }
                stopwatch.Stop();

                MessageBox.Show(String.Format("{0} byte toplam boyutunda {1} dosya {2} milisaniyede okundu.", totalbytes, totalfiles, stopwatch.ElapsedMilliseconds));
            }

            int errors = 0;
            foreach(var entry in files_to_put)
            {
                if (!entry.Value.SequenceEqual(results[entry.Key]))
                {
                    errors++;
                }
            }

            MessageBox.Show(String.Format("{0} hatalı sonuç", errors));
        }

        private void button5_Click(object sender, EventArgs e)
        {
            string[] files = Directory.GetFiles(src);
            string[] recent_files = new string[5];

            var i = 0;
            Random rnd = new Random();

            using (var db = new SimpleStorage.Database(dest))
            {
                foreach (string f in files)
                {
                    i++;
                    if (i % 17 == 0)
                    {
                        db.Remove("files", recent_files[rnd.Next(0, 4)]);
                    }

                    db.Put("files", f, File.ReadAllBytes(f));
                    if(i % 3 == 0)
                        recent_files[i % 5] = f;
                }

                MessageBox.Show("Finished insert/delete, validating data");
                i = 0;
                int k = 0;
                foreach (string f in files)
                {

                    byte[] result = db.Get("files", f);
                    if (result != null && !result.SequenceEqual(File.ReadAllBytes(f)))
                        k++;
                    else
                        i++;


                }
                MessageBox.Show(String.Format("{0} başarılı, {1} hatalı", i, k));

            }


        }
    }
}
