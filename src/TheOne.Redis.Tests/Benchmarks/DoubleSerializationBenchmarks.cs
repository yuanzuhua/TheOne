using System;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using NUnit.Framework;
using TheOne.Redis.External;

namespace TheOne.Redis.Tests.Benchmarks {

    [TestFixture]
    internal sealed class DoubleSerializationBenchmarks {

        private const int _times = 100000;

        public void Reset() {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }

        private static void PrintLastValues(string[] results, int count) {
            var sb = new StringBuilder();
            for (var i = _times - 1; i >= _times - count; i--) {
                sb.AppendLine(results[i]);
            }

            Console.WriteLine("Last {0} values: ", count);
            Console.WriteLine(sb);
        }

        [Test]
        public void Compare_double_serializers() {
            var initialVal = 0.3333333333333333d;

            var results = new string[_times];

            this.Reset();
            var sw = Stopwatch.StartNew();

            for (var i = 0; i < _times; i++) {
                results[i] = (initialVal + i).ToString();
            }

            Console.WriteLine("double.ToString(): Completed in ms: " + sw.ElapsedMilliseconds);
            // PrintLastValues(results, 100);

            this.Reset();
            sw = Stopwatch.StartNew();

            for (var i = 0; i < _times; i++) {
                results[i] = (initialVal + i).ToString("r");
            }

            Console.WriteLine("double.ToString('r') completed in ms: " + sw.ElapsedMilliseconds);
            // PrintLastValues(results, 100);

            // Default
            this.Reset();
            sw = Stopwatch.StartNew();

            for (var i = 0; i < _times; i++) {
                results[i] = DoubleConverter.ToExactString(initialVal + i);
            }

            Console.WriteLine("DoubleConverter.ToExactString(): Completed in ms: " + sw.ElapsedMilliseconds);
            // PrintLastValues(results, 100);

            // What #XBOX uses
            this.Reset();
            sw = Stopwatch.StartNew();

            for (var i = 0; i < _times; i++) {
                results[i] = BitConverter.ToString(BitConverter.GetBytes(initialVal + i));
            }

            Console.WriteLine("BitConverter.ToString() completed in ms: " + sw.ElapsedMilliseconds);
            // PrintLastValues(results, 100); 


            // What Book Sleeve uses
            this.Reset();
            sw = Stopwatch.StartNew();

            for (var i = 0; i < _times; i++) {
                results[i] = (initialVal + i).ToString("G", CultureInfo.InvariantCulture);
            }

            Console.WriteLine("double.ToString('G') completed in ms: " + sw.ElapsedMilliseconds);
            // PrintLastValues(results, 100); 
        }

    }

}
