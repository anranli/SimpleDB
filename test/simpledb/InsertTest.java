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
public class InsertTest extends TestUtil.CreateHeapFile {

  private DbIterator scan1;
<<<<<<< HEAD
  private TransactionId tid;
=======
  private DbIterator scan2;
  private DbIterator scan3;
  private TransactionId tid;
  private TransactionId tid2;
  private TransactionId tid3;
>>>>>>> 3320db877d03d7a951a257e4cfb5ce484d4887c3

  /**
   * Initialize each unit test
   */
  @Before public void setUp() throws Exception {
    super.setUp();
<<<<<<< HEAD
    this.scan1 = TestUtil.createTupleList(2,
        new int[] { 1, 2,
                    1, 4,
                    1, 6,
                    3, 2,
                    3, 4,
                    3, 6,
                    5, 7 });
    tid = new TransactionId();
=======
    int[] temp = new int[5000];
    for (int i = 0; i < 5000; i++){
      temp[i] = i;
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
    tid = new TransactionId();

>>>>>>> 3320db877d03d7a951a257e4cfb5ce484d4887c3
  }

  /**
   * Unit test for Insert.getTupleDesc()
   */
  @Test public void getTupleDesc() throws Exception {
    Insert op = new Insert(tid,scan1, empty.getId());
    TupleDesc expected = Utility.getTupleDesc(1);
    TupleDesc actual = op.getTupleDesc();
    assertEquals(expected, actual);
<<<<<<< HEAD
=======

    Delete op2 = new Delete(tid2,scan1);
    TupleDesc expected2 = Utility.getTupleDesc(1);
    TupleDesc actual2 = op2.getTupleDesc();
    assertEquals(expected2, actual2);
  }

  @Test public void DbException() throws Exception {
    String[] temp2 = new String[5000];
    for (int i = 0; i < 5000; i++){
      temp2[i] = "A" + i;
    }

    DbIterator scan2 = TestUtil.createTupleList(2, temp2 );
    try {
      Insert op = new Insert(tid,scan2, empty.getId());
      throw new Exception();
    }
    catch (DbException db){

    }
  }

  @Test public void fetchNext() throws Exception {
    Insert op = new Insert(tid,scan1, empty.getId());
    op.open();
    assertEquals(new IntField(2500), op.fetchNext().getField(0));
    assertEquals(null, op.fetchNext());

    Delete op2 = new Delete(tid2,scan1);
    op2.open();
    assertEquals(new IntField(2500), op2.fetchNext().getField(0));
    assertEquals(null, op2.fetchNext());
  }

  @Test public void children() throws Exception {
    Insert op = new Insert(tid,scan1, empty.getId());
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
>>>>>>> 3320db877d03d7a951a257e4cfb5ce484d4887c3
  }

  /**
   * Unit test for Insert.getNext(), inserting elements into an empty file
   */
  @Test public void getNext() throws Exception {
    Insert op = new Insert(tid,scan1, empty.getId());
    op.open();
    assertTrue(TestUtil.compareTuples(
<<<<<<< HEAD
        Utility.getHeapTuple(7, 1), // the length of scan1
        op.next()));

    // we should fit on one page
    assertEquals(1, empty.numPages());
=======
        Utility.getHeapTuple(2500, 1), // the length of scan1
        op.next()));

    // we should fit on one page
    assertEquals(5, empty.numPages());
>>>>>>> 3320db877d03d7a951a257e4cfb5ce484d4887c3
  }

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(InsertTest.class);
  }
}

