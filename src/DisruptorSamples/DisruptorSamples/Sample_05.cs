using Disruptor;
using System;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DisruptorSamples
{
    [TestClass]
    public class Sample_05
    {
        private const ulong MAX_ITERATIONS = 1_000;

        /// <summary>
        /// Sample 05: Disruptor with two Readers, a single Producer, and a Translator.
        /// </summary>
        [TestMethod]
        public void Sample_05_DR_SP_Tx()
        {
            var disruptor = NewDisruptor();

            disruptor.HandleEventsWith(
                new ThreadSleepRandomEventHandler(),
                new ThreadSleepConstantEventHandler());

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