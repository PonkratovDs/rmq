package queue

import (
	"testing"
	"time"

	"github.com/adjust/goenv"

	. "github.com/adjust/gocheck"
)

func TestCleanerSuite(t *testing.T) {
	TestingSuiteT(&CleanerSuite{}, t)
}

type CleanerSuite struct {
	goenv *goenv.Goenv
}

func (suite *CleanerSuite) SetUpSuite(c *C) {
	suite.goenv = goenv.TestGoenv()
}

func (suite *CleanerSuite) TestCleaner(c *C) {
	host, port, db := suite.goenv.GetRedis()
	flushConn := OpenConnection("cleaner-flush", host, port, db)
	flushConn.flushDb()
	flushConn.StopHeartbeat()

	conn := OpenConnection("cleaner-conn1", host, port, db)
	c.Check(conn.GetOpenQueues(), HasLen, 0)
	queue := conn.OpenQueue("q1")
	c.Check(conn.GetOpenQueues(), HasLen, 1)
	conn.OpenQueue("q2")
	c.Check(conn.GetOpenQueues(), HasLen, 2)

	c.Check(queue.ReadyCount(), Equals, 0)
	queue.Publish("del1")
	c.Check(queue.ReadyCount(), Equals, 1)
	queue.Publish("del2")
	c.Check(queue.ReadyCount(), Equals, 2)
	queue.Publish("del3")
	c.Check(queue.ReadyCount(), Equals, 3)
	queue.Publish("del4")
	c.Check(queue.ReadyCount(), Equals, 4)
	queue.Publish("del5")
	c.Check(queue.ReadyCount(), Equals, 5)
	queue.Publish("del6")
	c.Check(queue.ReadyCount(), Equals, 6)

	c.Check(queue.UnackedCount(), Equals, 0)
	queue.StartConsuming(2)
	time.Sleep(time.Millisecond)
	c.Check(queue.UnackedCount(), Equals, 3) // bufferSize+1
	c.Check(queue.ReadyCount(), Equals, 3)

	consumer := NewTestConsumer("c-A")
	consumer.AutoFinish = false
	consumer.AutoAck = false
	queue.AddConsumer("consumer1", consumer)
	time.Sleep(time.Millisecond)
	c.Check(queue.UnackedCount(), Equals, 4) // moves first delivery to consumer and unacks the next ready
	c.Check(queue.ReadyCount(), Equals, 2)

	c.Assert(consumer.LastDelivery, NotNil)
	c.Check(consumer.LastDelivery.Payload(), Equals, "del1")
	c.Check(consumer.LastDelivery.Ack(), Equals, true)
	time.Sleep(time.Millisecond)
	c.Check(queue.UnackedCount(), Equals, 3)
	c.Check(queue.ReadyCount(), Equals, 2)

	consumer.Finish()
	time.Sleep(time.Millisecond)
	c.Check(queue.UnackedCount(), Equals, 4)
	c.Check(queue.ReadyCount(), Equals, 1)
	c.Check(consumer.LastDelivery.Payload(), Equals, "del2")

	queue.StopConsuming()
	conn.StopHeartbeat()
	time.Sleep(time.Millisecond)

	conn = OpenConnection("cleaner-conn1", host, port, db)
	queue = conn.OpenQueue("q1")

	queue.Publish("del7")
	c.Check(queue.ReadyCount(), Equals, 2)
	queue.Publish("del7")
	c.Check(queue.ReadyCount(), Equals, 3)
	queue.Publish("del8")
	c.Check(queue.ReadyCount(), Equals, 4)
	queue.Publish("del9")
	c.Check(queue.ReadyCount(), Equals, 5)
	queue.Publish("del10")
	c.Check(queue.ReadyCount(), Equals, 6)

	c.Check(queue.UnackedCount(), Equals, 0)
	queue.StartConsuming(2)
	time.Sleep(time.Millisecond)
	c.Check(queue.UnackedCount(), Equals, 3)
	c.Check(queue.ReadyCount(), Equals, 3)

	consumer = NewTestConsumer("c-B")
	consumer.AutoFinish = false
	consumer.AutoAck = false
	queue.AddConsumer("consumer2", consumer)
	time.Sleep(time.Millisecond)
	c.Check(queue.UnackedCount(), Equals, 4)
	c.Check(queue.ReadyCount(), Equals, 2)
	c.Check(consumer.LastDelivery.Payload(), Equals, "del6")

	consumer.Finish() // unacked
	time.Sleep(time.Millisecond)
	c.Check(queue.UnackedCount(), Equals, 4)
	c.Check(queue.ReadyCount(), Equals, 1)
	c.Check(consumer.LastDelivery.Payload(), Equals, "del7")

	c.Check(consumer.LastDelivery.Ack(), Equals, true)
	c.Check(queue.UnackedCount(), Equals, 4)
	c.Check(queue.ReadyCount(), Equals, 1)

	queue.StopConsuming()
	conn.StopHeartbeat()
	time.Sleep(time.Millisecond)

	cleanerConn := OpenConnection("cleaner-conn", host, port, db)
	cleaner := NewCleaner(cleanerConn)
	c.Check(cleaner.Clean(), IsNil)
	c.Check(queue.ReadyCount(), Equals, 9) // 2/11 were acked above
	c.Check(conn.GetOpenQueues(), HasLen, 2)

	conn = OpenConnection("cleaner-conn1", host, port, db)
	queue = conn.OpenQueue("q1")
	queue.StartConsuming(10)
	consumer = NewTestConsumer("c-C")

	queue.AddConsumer("consumer3", consumer)
	time.Sleep(time.Millisecond)
	c.Check(consumer.LastDeliveries, HasLen, 9)

	queue.StopConsuming()
	conn.StopHeartbeat()
	time.Sleep(time.Millisecond)

	c.Check(cleaner.Clean(), IsNil)
	cleanerConn.StopHeartbeat()
}
