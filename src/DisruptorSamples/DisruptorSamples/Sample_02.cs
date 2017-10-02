using Disruptor;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;

namespace DisruptorSamples
{
    [TestClass]
    public class Sample_02
    {
        private const ulong MAX_ITERATIONS = 1_000_000;

        private static Disruptor.Dsl.Disruptor<TheRingBufferSlotType> _disruptor = null;

        private static RingBuffer<TheRingBufferSlotType> _ringbuffer = null;

        [ClassInitialize]
        public static void Class_Init(TestContext ctx)
        {
            _disruptor = NewDisruptor();

            _disruptor.HandleEventsWith(new SummingEventHandler());

            _ringbuffer = _disruptor.Start();
        }

        [ClassCleanup]
        public static void Class_Cleanup()
        {
            _disruptor.Shutdown();
        }

        /// <summary>
        /// Sample 02: Disruptor with single Reader,
        /// refactored to move .Start and .Shutdown into Test Class Init/Cleanup.
        /// </summary>
        [TestMethod]
        public void Sample_02_SR()
        {
            PublishTo(_ringbuffer);
        }

        private static Disruptor.Dsl.Disruptor<TheRingBufferSlotType> NewDisruptor()
        {
            return new Disruptor.Dsl.Disruptor<TheRingBufferSlotType>(
                () => new TheRingBufferSlotType(),
                64,
                TaskScheduler.Default);
        }

        internal class TheRingBufferSlotType
        {
            public byte TheValue;
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
                long sequenceNumber = ringbuffer.Next();
                var slot = ringbuffer[sequenceNumber];
                slot.TheValue = 1;
                ringbuffer.Publish(sequenceNumber);
            }
        }
    }
}