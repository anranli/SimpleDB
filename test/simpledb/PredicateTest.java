package simpledb;

import org.junit.Test;

import simpledb.systemtest.SimpleDbTestBase;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import junit.framework.JUnit4TestAdapter;

public class PredicateTest extends SimpleDbTestBase{

  /**
   * Unit test for Predicate.filter()
   */
  @Test public void filter() {
    int[] vals = new int[] { -2, -1, 0, 1, 2 };

    for (int i : vals) {
      Predicate p = new Predicate(0, Predicate.Op.EQUALS, TestUtil.getField(i));
      System.out.println(p.toString());
      assertFalse(p.filter(Utility.getHeapTuple(i - 1)));
      assertTrue(p.filter(Utility.getHeapTuple(i)));
      assertFalse(p.filter(Utility.getHeapTuple(i + 1)));
      assertEquals(p.getField(), 0);
      assertEquals(p.getOp(), Predicate.Op.EQUALS);
      assertEquals(p.getOperand(), TestUtil.getField(i));
    }

    for (int i : vals) {
      Predicate p = new Predicate(0, Predicate.Op.GREATER_THAN,
          TestUtil.getField(i));
      assertFalse(p.filter(Utility.getHeapTuple(i - 1)));
      assertFalse(p.filter(Utility.getHeapTuple(i)));
      assertTrue(p.filter(Utility.getHeapTuple(i + 1)));
      assertEquals(p.getField(), 0);
      assertEquals(p.getOp(), Predicate.Op.GREATER_THAN);
      assertEquals(p.getOperand(), TestUtil.getField(i));
    }

    for (int i : vals) {
      Predicate p = new Predicate(0, Predicate.Op.GREATER_THAN_OR_EQ,
          TestUtil.getField(i));
      assertFalse(p.filter(Utility.getHeapTuple(i - 1)));
      assertTrue(p.filter(Utility.getHeapTuple(i)));
      assertTrue(p.filter(Utility.getHeapTuple(i + 1)));
      assertEquals(p.getField(), 0);
      assertEquals(p.getOp(), Predicate.Op.GREATER_THAN_OR_EQ);
      assertEquals(p.getOperand(), TestUtil.getField(i));
    }

    for (int i : vals) {
      Predicate p = new Predicate(0, Predicate.Op.LESS_THAN,
          TestUtil.getField(i));
      assertTrue(p.filter(Utility.getHeapTuple(i - 1)));
      assertFalse(p.filter(Utility.getHeapTuple(i)));
      assertFalse(p.filter(Utility.getHeapTuple(i + 1)));
      assertEquals(p.getField(), 0);
      assertEquals(p.getOp(), Predicate.Op.LESS_THAN);
      assertEquals(p.getOperand(), TestUtil.getField(i));
    }

    for (int i : vals) {
      Predicate p = new Predicate(0, Predicate.Op.LESS_THAN_OR_EQ,
          TestUtil.getField(i));
      assertTrue(p.filter(Utility.getHeapTuple(i - 1)));
      assertTrue(p.filter(Utility.getHeapTuple(i)));
      assertFalse(p.filter(Utility.getHeapTuple(i + 1)));
      assertEquals(p.getField(), 0);
      assertEquals(p.getOp(), Predicate.Op.LESS_THAN_OR_EQ);
      assertEquals(p.getOperand(), TestUtil.getField(i));
    }
  }

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(PredicateTest.class);
  }
}

