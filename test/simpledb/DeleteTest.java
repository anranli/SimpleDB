package simpledb;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import junit.framework.JUnit4TestAdapter;

/**
 * We reserve more heavy-duty insertion testing for HeapFile and HeapPage.
 * This suite is superficial.
 */
public class DeleteTest extends TestUtil.CreateHeapFile {

  private DbIterator scan1;
  private DbIterator scan2;
  private DbIterator scan3;
  private TransactionId tid;
  private TransactionId tid2;
  private TransactionId tid3;

  /**
   * Initialize each unit test
   */
  @Before public void setUp() throws Exception {
    super.setUp();
    int[] temp = new int[5000];
    for (int i = 0; i < 5000; i++){
      temp[i] = i;
    }

    int[] temp2 = new int[2500];
    for (int i = 0; i < 2500; i++){
      temp[i] = i;
    }
    int[] temp3 = new int[2510];
    for (int i = 2500; i < 5010; i++){
      temp3[i - 2500] = i;
    }

    this.scan1 = TestUtil.createTupleList(2, temp );
        // new int[] { 1, 2,
        //             1, 4,
        //             1, 6,
        //             3, 2,
        //             3, 4,
        //             3, 6,
        //             5, 7,
        //             8, 4 });
    this.scan2 = TestUtil.createTupleList(2, temp2 );
    this.scan3 = TestUtil.createTupleList(2, temp3 );

    tid = new TransactionId();
    tid2 = new TransactionId();
    tid3 = new TransactionId();
  }

  /**
   * Unit test for Insert.getTupleDesc()
   */
  @Test public void getTupleDesc() throws Exception {
    Delete op = new Delete(tid,scan1);
    TupleDesc expected = Utility.getTupleDesc(1);
    TupleDesc actual = op.getTupleDesc();
    assertEquals(expected, actual);
  }


  @Test public void fetchNext() throws Exception {
    Insert op = new Insert(tid,scan1, empty.getId());
    op.open();
    assertEquals(new IntField(2500), op.fetchNext().getField(0));
    assertEquals(null, op.fetchNext());

    Delete op2 = new Delete(tid2,scan2);
    op2.open();
    assertEquals(new IntField(1250), op2.fetchNext().getField(0));
    assertEquals(null, op2.fetchNext());

    Delete op3 = new Delete(tid3,scan3);
    op3.open();
    assertEquals(new IntField(1250), op3.fetchNext().getField(0));
    assertEquals(null, op3.fetchNext());
  }

  @Test public void children() throws Exception {
    Delete op = new Delete(tid,scan1);
    DbIterator scan2 = TestUtil.createTupleList(2,
        new int[] { 1, 2,
                    1, 4,
                    1, 6,
                    3, 2,
                    3, 4,
                    3, 6,
                    5, 7,
                    8, 4 });
    assertEquals(scan1, op.getChildren()[0]);
    DbIterator[] temp = new DbIterator[1];
    temp[0] = scan2;
    op.setChildren(temp);
    assertEquals(scan2, op.getChildren()[0]);
  }

  /**
   * Unit test for Insert.getNext(), inserting elements into an empty file
   */
  @Test public void getNext() throws Exception {
    Insert op = new Insert(tid,scan1, empty.getId());
    op.open();
    assertTrue(TestUtil.compareTuples(
        Utility.getHeapTuple(2500, 1), // the length of scan1
        op.next()));

    // we should fit on one page
    assertEquals(5, empty.numPages());
  }

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(InsertTest.class);
  }
}

