using Disruptor;
using System;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DisruptorSamples
{
    [TestClass]
    public class Sample_06
    {
        private const ulong MAX_ITERATIONS = 1_000;

        private static Disruptor.Dsl.Disruptor<TheRingBufferSlotType> _disruptor = null;

        private static RingBuffer<TheRingBufferSlotType> _ringbuffer = null;

        private static EventProducerWithTranslator _producer = null;

        [ClassInitialize]
        public static void Class_Init(TestContext ctx)
        {
            _disruptor = NewDisruptor();

            _disruptor.HandleEventsWith(
                new ThreadSleepRandomEventHandler(),
                new ThreadSleepConstantEventHandler());

            _ringbuffer = _disruptor.Start();

            _producer = new EventProducerWithTranslator(_ringbuffer);
        }

        [ClassCleanup]
        public static void Class_Cleanup()
        {
            _disruptor.Shutdown();
        }

        /// <summary>
        /// Sample 06: Disruptor with two Readers, a single Producer, and a Translator,
        /// refactored to move .Start and .Shutdown into Test Class Init/Cleanup.
        /// </summary>
        [TestMethod]
        public void Sample_06_DR_SP_Tx()
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

        internal class ThreadSleepRandomEventHandler : IEventHandler<TheRingBufferSlotType>
        {
            private Random rnd = new Random();

            public void OnEvent(TheRingBufferSlotType data, long sequence, bool endOfBatch)
            {
                Thread.Sleep(rnd.Next(1, 5));
            }
        }

        internal class ThreadSleepConstantEventHandler : IEventHandler<TheRingBufferSlotType>
        {
            public int ThreadSleepInterval { get; set; } = 2;

            public void OnEvent(TheRingBufferSlotType data, long sequence, bool endOfBatch)
            {
                Thread.Sleep(ThreadSleepInterval);
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