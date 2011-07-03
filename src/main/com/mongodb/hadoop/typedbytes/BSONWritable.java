package com.mongodb.hadoop.typedbytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.typedbytes.TypedBytesWritable;
import org.bson.BSONCallback;
import org.bson.BSONDecoder;
import org.bson.BSONEncoder;
import org.bson.BSONObject;
import org.bson.BasicBSONCallback;
import org.bson.BasicBSONObject;
import org.bson.io.BasicOutputBuffer;

public class BSONWritable extends TypedBytesWritable implements BSONObject {
	
	/**
     * Constructs a new instance.
     */
    public BSONWritable() {
        _doc = new BasicBSONObject();
    }

    /**
     * Copy constructor, copies data from an existing BSONWritable
     * 
     * @param other
     *            The BSONWritable to copy from
     */
    public BSONWritable(BSONWritable other) {
        this();
        copy( other );
    }

	@Override
	public boolean containsField(String fieldName) {
		return _doc.containsField( fieldName );
	}

	@Override
	public boolean containsKey(String key) {
		return _doc.containsKey( key );
	}

	@Override
	public Object get(String key) {
		return _doc.get( key );
	}

	@Override
	public Set<String> keySet() {
		return _doc.keySet();
	}

	@Override
	public Object put(String key, Object value) {
		return _doc.put( key, value );
	}

	@Override
	public void putAll(BSONObject otherMap) {
		_doc.putAll( otherMap );

	}

	@Override
	public void putAll(Map arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public Object removeField(String key) {
		return _doc.removeField( key );
	}

	@Override
	public Map toMap() {
		return _doc.toMap();
	}
	
	/**
     * {@inheritDoc}
     * 
     * @see Writable#write(DataOutput)
     */
    public void write( DataOutput out ) throws IOException{
        BSONEncoder enc = new BSONEncoder();
        BasicOutputBuffer buf = new BasicOutputBuffer();
        enc.set( buf );
        enc.putObject( _doc );
        enc.done();
        out.writeInt(buf.size());
        //For better performance we can copy BasicOutputBuffer.pipe(OutputStream)
        //to have a method signature that works with DataOutput
        out.write( buf.toByteArray() );
    }

    /**
     * {@inheritDoc}
     * 
     * @see Writable#readFields(DataInput)
     */
    public void readFields( DataInput in ) throws IOException{
        BSONDecoder dec = new BSONDecoder();
        BSONCallback cb = new BasicBSONCallback();
        // Read the BSON length from the start of the record
        int dataLen = in.readInt();
        byte[] buf = new byte[dataLen];
        in.readFully( buf );
        dec.decode( buf, cb );
        _doc = (BSONObject) cb.get();
        log.info( "Decoded a BSON Object: " + _doc );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString(){
        BSONEncoder enc = new BSONEncoder();
        BasicOutputBuffer buf = new BasicOutputBuffer();
        enc.set( buf );
        enc.putObject( _doc );
        enc.done();
        String str = buf.asString();
        log.debug( "Output As String: '" + str + "'" );
        return str;
    }
	
	/** Used by child copy constructors. */
    protected synchronized void copy( Writable other ){
        if ( other != null ) {
            try {
                DataOutputBuffer out = new DataOutputBuffer();
                other.write( out );
                DataInputBuffer in = new DataInputBuffer();
                in.reset( out.getData(), out.getLength() );
                readFields( in );

            }
            catch ( IOException e ) {
                throw new IllegalArgumentException( "map cannot be copied: " + e.getMessage() );
            }

        }
        else {
            throw new IllegalArgumentException( "source map cannot be null" );
        }
    }
    
    protected BSONObject _doc;

    private static final Log log = LogFactory.getLog( BSONWritable.class );

}
