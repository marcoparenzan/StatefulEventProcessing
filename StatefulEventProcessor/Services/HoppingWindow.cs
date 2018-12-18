using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace StatefulEventProcessor.Services
{
    public partial class HoppingWindow
    {
        public TimeSpan WindowSize { get; private set; }
        public TimeSpan HopSize { get; private set; }
        private long HopSize100ns { get; set; }
        public DateTime StartTime { get; private set; }
        private long StartFileTime { get; set; }

        private int Repeats { get; set; }

        private Func<DateTime, int, Task> HopHandler { get; set; }

        private HoppingWindow()
        {
        }

        public static HoppingWindow New(TimeSpan windowSize, TimeSpan? hopSize = null)
        {
            var x = new HoppingWindow();
            x.WindowSize = windowSize;
            if (hopSize == null)
                x.HopSize = x.WindowSize;
            else
                x.HopSize = hopSize.Value;
            x.HopSize100ns = (long) (x.HopSize.TotalSeconds * 10000000);
            x.Repeats = (int) (x.WindowSize.TotalSeconds / x.HopSize.TotalSeconds);

            x.StartTime = DateTime.Now;
            x.StartFileTime = x.StartTime.ToFileTime();
            return x;
        }

        public HoppingWindow From(DateTime? startTime = null)
        {
            if (startTime == null)
                this.StartTime = DateTime.Now;
            else
                this.StartTime = startTime.Value;
            return this;
        }

        public HoppingWindow HandleWith(Func<int, long, Task> hopHandler)
        {
            HopHandler = HopHandler;
            return this;
        }

        public HoppingWindow At(DateTime timestamp, Func<DateTime, int, Task> hopHandler = null)
        {
            var filetime = timestamp.ToFileTime();
            var windowTime = (long)(StartFileTime + ((filetime - StartFileTime) / HopSize100ns) * HopSize100ns);

            if (hopHandler == null) hopHandler = HopHandler;

            for (var hop = Repeats; hop > 0; hop--)
            {
                if (hopHandler != null)
                {
                    try
                    {
                        hopHandler(DateTime.FromFileTime(windowTime), hop);
                        
                    }
                    catch(Exception ex)
                    {
                        throw ex;
                    }
                    windowTime -= (long)WindowSize.TotalSeconds;
                }
            }
            return this;
        }
    }
}
