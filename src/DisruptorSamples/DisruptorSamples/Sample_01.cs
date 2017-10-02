using Disruptor;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;

namespace DisruptorSamples
{
    [TestClass]
    public class Sample_01
    {
        /// <summary>
        /// Sample 01: Disruptor with single Reader.
        /// </summary>
        [TestMethod]
        public void Sample_01_SR()
        {
            var d = NewDisruptor();

            d.HandleEventsWith(new SummingEventHandler());

            try
            {
                var ringbuffer = d.Start();
                PublishTo(ringbuffer);
            }
            finally
            {
                d.Shutdown();
            }
        }

        private Disruptor.Dsl.Disruptor<TheRingBufferSlotType> NewDisruptor()
        {
            return new Disruptor.Dsl.Disruptor<TheRingBufferSlotType>(
                () => new TheRingBufferSlotType(),
                64,
                TaskScheduler.Default);
        }

        internal class TheRingBufferSlotType
        {
            public byte TheValue { get; set; }
        }

        internal class SummingEventHandler : IEventHandler<TheRingBufferSlotType>
        {
            public ulong Sum { get; private set; } = 0;
            public void OnEvent(TheRingBufferSlotType data, long sequence, bool endOfBatch)
            {
                Sum += data.TheValue;
            }
        }

        private static void PublishTo(RingBuffer<TheRingBufferSlotType> ringbuffer)
        {
            for (ulong ix = 0; ix < MAX_ITERATIONS; ix++)
            {
                long seqNum = ringbuffer.Next();
                var slot = ringbuffer[seqNum];
                slot.TheValue = 1;
                ringbuffer.Publish(seqNum);
            }
        }

        private const ulong MAX_ITERATIONS = 1_000_000;
    }
}