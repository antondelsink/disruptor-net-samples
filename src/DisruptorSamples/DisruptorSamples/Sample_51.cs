using Disruptor;
using System.Diagnostics;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace DisruptorSamples
{
    [TestClass]
    public class Sample_51
    {
        private const ulong ITERATIONS = 1_000_000;

        [TestMethod]
        public void Sample_51_ReplicatingPair()
        {
            // Primary Disruptor Setup

            var primary_replicator = new ReplicatorEventHandler();
            var primary_businesslogic = new BusinessLogicEventHandler();

            var primary_disruptor = NewDisruptor();

            primary_disruptor
                .HandleEventsWith(primary_replicator)
                .Then(primary_businesslogic);

            // Replica Disruptor Setup

            var replica_businesslogic = new BusinessLogicEventHandler();

            var replica_disruptor = NewDisruptor();

            replica_disruptor.HandleEventsWith(replica_businesslogic);

            try // start Disruptors
            {
                var replica_ringbuffer = replica_disruptor.Start();
                primary_replicator.TargetRingBuffer = replica_ringbuffer;

                var primary_ringbuffer = primary_disruptor.Start();

                var stopwatch = Stopwatch.StartNew();

                // Mock Processing
                PublishTo(primary_ringbuffer, out long? lastPublishedSequenceNumber);
                Assert.IsNotNull(lastPublishedSequenceNumber);

                AssertWithTimeout.IsTrue(() => lastPublishedSequenceNumber == primary_replicator.LastReplicatedSequenceNumber,
                    "timed out waiting for replication to complete");

                AssertWithTimeout.IsTrue(() => lastPublishedSequenceNumber == primary_businesslogic.LastSeenSequenceNumber,
                    "timed out waiting for primary business logic to complete");

                AssertWithTimeout.IsTrue(() => lastPublishedSequenceNumber == replica_businesslogic.LastSeenSequenceNumber,
                    "timed out waiting for replica business logic to complete");

                stopwatch.Stop();
                Assert.IsTrue(stopwatch.ElapsedMilliseconds < 750, "too slow!");

                Assert.IsTrue(primary_businesslogic.LastSeenSequenceNumber == replica_businesslogic.LastSeenSequenceNumber);
            }
            finally
            {
                primary_disruptor.Shutdown();
                replica_disruptor.Shutdown();
            }
        }

        internal class TheRingBufferSlotType
        {
            public byte TheValue { get; set; }
        }

        private static Disruptor.Dsl.Disruptor<TheRingBufferSlotType> NewDisruptor()
        {
            return new Disruptor.Dsl.Disruptor<TheRingBufferSlotType>(
                () => new TheRingBufferSlotType(),
                64,
                TaskScheduler.Default);
        }

        private class ReplicatorEventHandler : IEventHandler<TheRingBufferSlotType>
        {
            public long? LastReplicatedSequenceNumber { get; private set; } = null;

            private Producer _producer = null;

            #region TargetRingBuffer
            private RingBuffer<TheRingBufferSlotType> _TargetRingBuffer = null;
            public RingBuffer<TheRingBufferSlotType> TargetRingBuffer
            {
                get
                {
                    if (_TargetRingBuffer == null)
                    {
                        throw new InvalidOperationException();
                    }
                    return _TargetRingBuffer;
                }
                set
                {
                    _TargetRingBuffer = value;
                    _producer = new Producer(value);
                }
            }
            #endregion

            public void OnEvent(TheRingBufferSlotType originSlot, long sequence, bool endOfBatch)
            {
                Assert.IsNotNull(this._TargetRingBuffer);

                _producer.OnNewData(originSlot.TheValue);

                this.LastReplicatedSequenceNumber = _TargetRingBuffer.Cursor;
            }
        }

        private class BusinessLogicEventHandler : IEventHandler<TheRingBufferSlotType>
        {
            public ulong Sum { get; private set; } = 0;
            public long? LastSeenSequenceNumber { get; private set; } = null;
            public void OnEvent(TheRingBufferSlotType data, long sequence, bool endOfBatch)
            {
                Sum += data.TheValue;
                this.LastSeenSequenceNumber = sequence;
            }
        }

        private static void PublishTo(RingBuffer<TheRingBufferSlotType> ringbuffer, out long? lastPublishedSequenceNumber)
        {
            var producer = new Producer(ringbuffer);

            for (ulong ix = 0; ix < ITERATIONS; ix++)
            {
                producer.OnNewData(1);
            }

            lastPublishedSequenceNumber = ringbuffer.Cursor;
        }

        internal class Producer
        {
            private RingBuffer<TheRingBufferSlotType> _ringbuffer;
            private IEventTranslatorOneArg<TheRingBufferSlotType, byte> _translator;

            public Producer(RingBuffer<TheRingBufferSlotType> targetRingBuffer)
            {
                _ringbuffer = targetRingBuffer;
                _translator = new AssignmentTranslator();
            }

            public void OnNewData(byte newData)
            {
                _ringbuffer.PublishEvent<byte>(_translator, newData);
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