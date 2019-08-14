package javatx.core;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import javatx.core.Transaction.Entity;
import javatx.core.Transaction.TransactionAbortedException;
import javatx.core.Transaction.TransactionException;
import junit.framework.Assert;
import junit.framework.TestCase;

public class TransactionTest extends TestCase{

    
    
    public void testEntityCommitAndRollback() throws Exception {
        Entity<Object> e = Entity.create("Hello");
        e.modify(s -> s + " Senthil")
        .commit()
        .read(assertObject("Hello Senthil")); //verify commit.
        
        e.modify(s -> s + " Ganesh")
        .commit()
        .rollback()
        .read(assertObject("Hello Senthil")); //verify rollback.
    }
    
    public void testTransactionCommitAborted() throws Exception {
        Entity<Object> e1 = Entity.create("Senthil");
        Entity<Object> e2 = Entity.create(100);
        Transaction t1 = Transaction.create(e1, e2);
        
        t1.begin()
        .modify(0, _0 -> _0 + " Ganesh")
        .modify(1, _1 -> (Integer) _1 - 50);
        
        e2.modify(s -> (Integer)s + 50).commit(); //e2 modified before t1 commit
        
        assertException(TransactionAbortedException.class, () -> t1.commit()); //assert abort exception  
        e1.read(assertObject("Senthil")); //assert old value retained for e1
        e2.read(assertObject(150)); //assert updated value (not by t1) retained for e2
    }
    
    public void testTransactionCommitRollback() throws Exception {
        Entity<Object> e1 = Entity.create("Hello");
        Entity<Object> e2 = Entity.create(100);
        Transaction.create(e1, e2)
        .begin()
        .modify(0, _1 -> _1 + " Senthil")
        .modify(1, _2 -> (Integer) _2 + 100)
        .commit()
        .rollback();
        
        e1.read(assertObject("Hello"));
        e2.read(assertObject(100));
    }
    
    public void testTransactionCommitRollbackAfterEntityUpdate() throws  Exception {
        Entity<Object> e1 = Entity.create("Hello");
        Entity<Object> e2 = Entity.create(100);
        Transaction tx = Transaction.create(e1, e2)
        .begin()
        .modify(0, _1 -> _1 + " Senthil")
        .modify(1, _2 -> (Integer) _2 + 100)
        .commit();
        
        CompletableFuture.runAsync(() -> {
            try {
                e2.modify(_x -> (Integer) _x - 50).commit();
            } catch (TransactionException e) {
                Assert.assertFalse(e.getLocalizedMessage(), true);
            }
        }).get();
        
        tx.rollback();
        
        e1.read(assertObject("Hello"));
        e2.read(assertObject(150)); //e2 should be un-affected by rollback of tx
    }
    
    @FunctionalInterface
    interface Block {
        void code() throws Exception;
    }
    
    private static<X extends Exception> void assertException(final Class<X> e, Block bl) {
        try {
            bl.code();
        } catch (Exception ex) {
            Assert.assertTrue("Expected exception of type " + e.getName() + " but got " + ex.getClass().getName(), ex.getClass().getName().equals(e.getName()));
        }
    }
    
    private static Consumer<Object> assertObject(final Object expected) {
        return actual -> 
        Assert.assertEquals(
            String.format("Expected %s but got %s", expected, actual), 
            expected, 
            actual);
    }
}
