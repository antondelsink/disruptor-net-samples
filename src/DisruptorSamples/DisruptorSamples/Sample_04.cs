using Disruptor;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;

namespace DisruptorSamples
{
    [TestClass]
    public class Sample_04
    {
        private const ulong MAX_ITERATIONS = 1_000_000;

        private static Disruptor.Dsl.Disruptor<TheRingBufferSlotType> _disruptor = null;

        private static RingBuffer<TheRingBufferSlotType> _ringbuffer = null;

        private static EventProducerWithTranslator _producer = null;

        [ClassInitialize]
        public static void Class_Init(TestContext ctx)
        {
            _disruptor = NewDisruptor();

            _disruptor.HandleEventsWith(new SummingEventHandler());

            _ringbuffer = _disruptor.Start();

            _producer = new EventProducerWithTranslator(_ringbuffer);
        }

        [ClassCleanup]
        public static void Class_Cleanup()
        {
            _disruptor.Shutdown();
        }

        /// <summary>
        /// Sample 04: Disruptor with single Reader, single Producer, and a Translator,
        /// refactored to move .Start and .Shutdown into Test Class Init/Cleanup.
        /// </summary>
        [TestMethod]
        public void Sample_04_SR_SP_Tx()
        {
            for (ulong ix = 0; ix < MAX_ITERATIONS; ix++)
            {
                _producer.OnNewData(1);
            }
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

        internal class EventProducerWithTranslator
        {
            private RingBuffer<TheRingBufferSlotType> _ringbuffer;
            private IEventTranslatorOneArg<TheRingBufferSlotType, byte> _translator;

            public EventProducerWithTranslator(RingBuffer<TheRingBufferSlotType> rb)
            {
                _ringbuffer = rb;
                _translator = new AssignmentTranslator();
            }

            public void OnNewData(byte newData)
            {
                _ringbuffer.PublishEvent(_translator, newData);
            }
        }

        internal class AssignmentTranslator : IEventTranslatorOneArg<TheRingBufferSlotType, byte>
        {
            public void TranslateTo(TheRingBufferSlotType slot, long sequence, byte theData)
            {
                slot.TheValue = theData;
            }
        }
    }
}