using Disruptor;
using System.Diagnostics;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DisruptorSamples
{
    [TestClass]
    public class Sample_50
    {
        private const ulong ITERATIONS = 1_000;

        [TestMethod]
        public void Sample_50_ReplicatingPair()
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
                Assert.IsTrue(stopwatch.ElapsedMilliseconds < 750, $"too slow! {stopwatch.ElapsedMilliseconds}ms");

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
            public byte TheValue;
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
            public RingBuffer<TheRingBufferSlotType> TargetRingBuffer { get; set; } = null;

            public void OnEvent(TheRingBufferSlotType originSlot, long sequence, bool endOfBatch)
            {
                Assert.IsNotNull(this.TargetRingBuffer);

                long seqNum = TargetRingBuffer.Next();
                var replicaSlot = TargetRingBuffer[seqNum];
                replicaSlot.TheValue = originSlot.TheValue;
                TargetRingBuffer.Publish(seqNum);

                this.LastReplicatedSequenceNumber = seqNum;
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
            lastPublishedSequenceNumber = null;
            for (ulong ix = 0; ix < ITERATIONS; ix++)
            {
                long seqNum = ringbuffer.Next();
                var slot = ringbuffer[seqNum];
                slot.TheValue = 1;
                ringbuffer.Publish(seqNum);

                lastPublishedSequenceNumber = seqNum;
            }
        }
    }
}