package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;

        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
    private final Type[] types;
    private final String[] fieldNames;


    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return new Iterator<TDItem>() {
            private int ids = -1;
            @Override
            public boolean hasNext() {
                return ids+1<types.length;
            }

            @Override
            public TDItem next() {
                if(++ids==types.length){
                    throw new NoSuchElementException();
                }else{
                    return new TDItem(types[ids],fieldNames[ids]);
                }
            }
        };
        // some code goes here

    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        types = new Type[typeAr.length];
        for(int i =0;i<typeAr.length;i++){
            types[i]=typeAr[i];
        }
        fieldNames = new String[fieldAr.length];
        for(int i=0;i<fieldAr.length;i++){
            fieldNames[i]=fieldAr[i];
        }
        // some code goes here
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this(typeAr,new String[0]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return types.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i<0||i>=types.length){
            throw new NoSuchElementException("index out of array");
        }else {
            if(i>=fieldNames.length){
                return new String("");
            }else{
                return this.fieldNames[i];
            }
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if(i<0||i>=types.length){
            throw new NoSuchElementException("index out of array");
        }else {
            return types[i];
        }
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if(null!=name){
            for(int i=0;i<fieldNames.length;i++){
                if(name.equals(fieldNames[i])){
                    return i;
                }
            }
        }
        throw new NoSuchElementException("not find name");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for(int i=0;i<types.length;i++){
            size += types[i].getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        Type[] newType = new Type[td1.types.length + td2.types.length];
        String[] newFieldName = new String[td1.types.length + td2.types.length];
        int j = 0 ;
        for(int i=0 ; i<td1.types.length ; i++){
            newType[j] = td1.types[i];
            if(i<td1.fieldNames.length){
                newFieldName[j++] = td1.fieldNames[i];
            }else{
                newFieldName[j++] = new String();
            }
        }
        for(int i=0 ; i<td2.types.length ; i++){
            newType[j] = td2.types[i];
            if(i<td1.fieldNames.length){
                newFieldName[j++] = td2.fieldNames[i];
            }else{
                newFieldName[j++] = new String();
            }
        }
        return new TupleDesc(newType,newFieldName);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if(!(o instanceof TupleDesc)){
            return false;
        }else{
            TupleDesc newTupleDesc = (TupleDesc) o;
            if(newTupleDesc.numFields()!=this.numFields()){
                return false;
            }
            if(newTupleDesc.fieldNames.length==0){
                if(this.fieldNames.length!=0){
                    return false;
                }else{
                    for(int i = 0;i<this.types.length;i++) {
                        if(this.types[i]!=null){
                            if(!this.types[i].equals(newTupleDesc.types[i])){
                                return false;
                            }
                        }else{
                            if(newTupleDesc.types[i]!=null){
                                return false;
                            }
                        }
                    }
                    return true;
                }
            }
            for(int i = 0;i<this.fieldNames.length;i++){
                if(!this.fieldNames[i].equals(newTupleDesc.fieldNames[i])){
                    return false;
                }
                if(!this.types[i].equals(newTupleDesc.types[i])){
                    return false;
                }
            }
            return true;
        }
        // some code goes here
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder bd = new StringBuilder();
        int n = this.numFields();
        bd.append(getFieldType(0));
        bd.append("("+this.getFieldName(0)+")");
        for(int i=1;i<n;i++){
            bd.append(","+getFieldType(i));
            bd.append("("+this.getFieldName(i)+"),");
        }

        return bd.toString();
    }
}
