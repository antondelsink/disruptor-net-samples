using Disruptor;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;

namespace DisruptorSamples
{
    [TestClass]
    public class Sample_03
    {
        private const ulong MAX_ITERATIONS = 1_000_000;

        /// <summary>
        /// Sample 03: Disruptor with single Reader, single Producer, and a Translator.
        /// </summary>
        [TestMethod]
        public void Sample_03_SR_SP_Tx()
        {
            var disruptor = NewDisruptor();

            disruptor.HandleEventsWith(new SummingEventHandler());

            try
            {
                var ringbuffer = disruptor.Start();

                var producer = new EventProducerWithTranslator(ringbuffer);

                for (ulong ix = 0; ix < MAX_ITERATIONS; ix++)
                {
                    producer.OnNewData(1);
                }
            }
            finally
            {
                disruptor.Shutdown();
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