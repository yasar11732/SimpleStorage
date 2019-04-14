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
            f.RootFolder = Environment.SpecialFolder.ApplicationData;
            if(f.ShowDialog() == DialogResult.OK)
            {
                src = f.SelectedPath;
                textBox1.Text = src;
            }
        }

        private void button2_Click(object sender, EventArgs e)
        {
            var f = new FolderBrowserDialog();
            f.RootFolder = Environment.SpecialFolder.ApplicationData;
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
            var src_save = src;
            var dbname = Path.GetFileName(src_save);

            int totalbytes = 0;
            int totalfiles = 0;
            int not_found = 0;
            int success = 0;
            int error = 0;

            var stopwatch = new Stopwatch();

            using (var db = new SimpleStorage.Database(dest))
            {

                foreach(string f in GetFiles(src))
                {
                    totalfiles++;
                    var data = File.ReadAllBytes(f);
                    var _f = f.Substring(src_save.Length + 1);

                    stopwatch.Start();
                    var response = db.Get(dbname, _f);
                    stopwatch.Stop();
                    if(response == null)
                    {
                        not_found++;
                        continue;
                    }
                    totalbytes += response.Length;
                    if (data.SequenceEqual(response))
                        success++;
                    else
                        error++;

                }

                
            }

            MessageBox.Show(String.Format("{0} byte toplam boyutunda {1} dosya {2} milisaniyede okundu. Başarılı: {3}, Hatalı: {4}, Bulunamadı: {5}", totalbytes, totalfiles, stopwatch.ElapsedMilliseconds, success, error, not_found));
        }

        static IEnumerable<string> GetFiles(string path)
        {
            Queue<string> queue = new Queue<string>();
            queue.Enqueue(path);
            while (queue.Count > 0)
            {
                path = queue.Dequeue();
                try
                {
                    foreach (string subDir in Directory.GetDirectories(path))
                    {
                        queue.Enqueue(subDir);
                    }
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine(ex);
                }
                string[] files = null;
                try
                {
                    files = Directory.GetFiles(path);
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine(ex);
                }
                if (files != null)
                {
                    for (int i = 0; i < files.Length; i++)
                    {
                        yield return files[i];
                    }
                }
            }
        }

        private void button5_Click(object sender, EventArgs e)
        {
            string[] recent_files = new string[23];
            var src_save = src;
            var dbname = Path.GetFileName(src_save);
            
            var i = 0;
            Random rnd = new Random();

            using (var db = new SimpleStorage.Database(dest))
            {
                foreach (string f in GetFiles(src_save))
                {
                    var _f = f.Substring(src_save.Length + 1);
                    i++;
                    if (i % 71 == 0)
                    {
                        db.Remove(dbname, recent_files[rnd.Next(0, 23)]);
                    }

                    db.Put(dbname, _f, File.ReadAllBytes(f));
                    if(i % 3 == 0)
                        recent_files[i % 23] = _f;
                }

                MessageBox.Show("Finished insert/delete, validating data");
                i = 0;
                int k = 0;
                foreach (string f in GetFiles(src))
                {
                    var _f = f.Substring(src_save.Length + 1);
                    byte[] result = db.Get(dbname, _f);
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
