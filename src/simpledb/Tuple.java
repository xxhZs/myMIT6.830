package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc td;
    private Field[] fields;
    private RecordId recordId;
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        this.td = td;
        fields = new Field[td.numFields()];
        // some code goes here
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.recordId = rid;
        // some code goes here
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if(i<0||i>fields.length){
            throw  new NoSuchElementException();
        }else{
            fields[i]=f;
        }
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        if(i<0||i>fields.length){
            throw  new NoSuchElementException();
        }else{
            return fields[i];
        }
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getField(0).toString());
        for(int i= 1; i<fields.length;i++){
            sb.append("\t"+this.getField(i).toString());
        }
        return sb.toString();
        // some code goes here
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return new Iterator<Field>() {
            private int ids=-1;
            @Override
            public boolean hasNext() {
                return ids+1>fields.length;
            }

            @Override
            public Field next() {
                if(++ids>fields.length){
                    throw new NoSuchElementException();
                }else{
                    return fields[ids];
                }
            }
        };
        // some code goes here
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        this.td = td;
        // some code goes here
    }

    public void getField() {
    }
}
