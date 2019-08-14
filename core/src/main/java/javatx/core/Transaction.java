package javatx.core;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

public interface Transaction {

    Transaction begin();

    Transaction modify(final int index, Function<Object, Object> action) throws IndexOutOfBoundsException;

    Transaction commit() throws TransactionException;

    Transaction rollback() throws TransactionException;

    static class TransactionException extends Exception {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        TransactionException() {
            super();
        }

        TransactionException(final String msg) {
            super(msg);
        }

        TransactionException(final Throwable e) {
            super(e);
        }

    }

    final static class TransactionAbortedException extends TransactionException {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        TransactionAbortedException() {
            super();
        }

        TransactionAbortedException(final String msg) {
            super(msg);
        }

        TransactionAbortedException(final Throwable e) {
            super(e);
        }
    }

    @SafeVarargs
    public static Transaction create(final Entity<Object>...entities) {
        return new Simple(Arrays.asList(entities));
    }


    final static class Simple implements Transaction {

        private final List<Entity<Object>> entities;

        private final List<Object> snapshot;

        Simple (final List<Entity<Object>> entities) {
            this.entities = entities;
            this.snapshot = new ArrayList<>();

        }

        @Override
        public Transaction begin() {
            entities.forEach(e -> e.read(snapshot::add));
            return this;
        }


        @Override
        public Transaction modify(final int index, final Function<Object, Object> action)
            throws IndexOutOfBoundsException {
            if (index < 0 || index >= entities.size())
                throw new IndexOutOfBoundsException(
                    String.format("allowed range for index is [%d..%d]", 0, entities.size() - 1));

            final Entity<Object> entity = entities.get(index);
            entity.read(o -> {
                snapshot.set(index, o); //set snapshot to recent value of entity.
                entity.modify(action); //modify thread local copy of the entity.
            });

            return this;
        }     

        @Override
        public Transaction commit() throws TransactionException {
            final AtomicBoolean snapshotAndPreCommitValue = new AtomicBoolean(true);
            final AtomicReference<TransactionException> excep = new AtomicReference<>();
            int failed = -1;
            for (int i = 0; i < entities.size(); i ++) {
                final int _i = i;
                final Entity<Object> entity = entities.get(i);
                entity.read(o -> {
                    snapshotAndPreCommitValue.set(snapshot.get(_i).equals(o)); //nobody else have modified the entity outside the tx.
                    if(snapshotAndPreCommitValue.get()) {
                        try {
                            entity.commit();
                            entity.read(_o -> snapshot.set(_i, _o)); //update snapshot after commit.
                        } catch (TransactionException e) {
                            excep.set(new TransactionAbortedException(e));
                        }                        
                    }
                });
                
                if (excep.get() != null) {
                    failed = i;
                    break;
                }
                if (!snapshotAndPreCommitValue.get()) {
                    failed = i;
                    break;
                }
            }

            for(int i = 0; i < failed; i ++){
                try {
                    entities.get(i).rollback();
                } catch (TransactionException e) {
                    excep.set(e);
                }
            }
            if (failed != -1) {
                if (excep.get() != null)
                    throw excep.get();
                throw new TransactionAbortedException("Please retry the transaction");
            }
            return this;
        }

        @Override
        public Transaction rollback() throws TransactionException {
            final AtomicReference<TransactionException> excep = new AtomicReference<>();
            for (int i = 0; i < entities.size(); i ++) {
                final Entity<Object> entity = entities.get(i);
                final int _i = i;
                entity.read(o -> {
                    if (snapshot.get(_i).equals(o)) {
                        try {
                            entity.rollback();
                        } catch (TransactionException e) {
                            excep.set(e);
                        }
                    }
                });
            }
            
            if (excep.get() != null)
                throw excep.get();
            
            return this;
        }
    }

    interface Entity<T> {
        Entity<T> modify(final Function<T, T> newValue); //update the value.
        Entity<T> commit() throws TransactionException;
        Entity<T> rollback() throws TransactionException;
        Entity<T> read(final Consumer<T> action);

        public static <R> Entity<R> create(final R initialValue) {
            return new Simple<>(initialValue);
        }

        final static class Simple<T> implements Entity<T> {
            private final AtomicReference<T> value;
            private ThreadLocal<T> uncommitted;
            private ThreadLocal<T> committed;

            Simple (final T value) {
                this.value = new AtomicReference<>(value);
                this.uncommitted = new ThreadLocal<T>() {
                    @Override
                    protected T initialValue() {
                        return value;
                    }
                };

                this.committed = new ThreadLocal<T>() {
                    @Override
                    protected T initialValue() {
                        return value;
                    }
                };
            }

            @Override
            public Entity<T> modify(final Function<T, T> fn) {
                committed.set(value.get()); //updated committed before modification.
                uncommitted.set(fn.apply(committed.get()));
                return this;
            }

            @Override
            public Entity<T> commit() throws TransactionException {
                T oldValue = committed.get();
                
                if (!oldValue.equals(value.get()))
                    throw new TransactionException("dirty read");
                
                committed.set(uncommitted.get());
                uncommitted.set(oldValue);
                value.set(committed.get());
                return this;
            }

            @Override
            public Entity<T> rollback() throws TransactionException{
                T oldValue = committed.get();

                if (!oldValue.equals(value.get()))
                    throw new TransactionException("dirty read");
                committed.set(uncommitted.get());
                uncommitted.set(oldValue);
                value.set(committed.get());
                return this;
            }

            @Override
            public Entity<T> read(final Consumer<T> action) {
                action.accept(value.get()); //committed read.
                return this;
            }
        }
    }
}