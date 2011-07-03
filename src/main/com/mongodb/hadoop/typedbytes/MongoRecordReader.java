package com.mongodb.hadoop.typedbytes;

import java.io.IOException;

import javax.sound.midi.MidiDevice.Info;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.record.Buffer;
import org.apache.hadoop.typedbytes.TypedBytesWritable;
import org.bson.BSONObject;

import com.mongodb.DBCursor;
import com.mongodb.hadoop.mapred.input.MongoInputSplit;

public class MongoRecordReader implements RecordReader<TypedBytesWritable, TypedBytesWritable> {

	public MongoRecordReader(MongoInputSplit split) {
        _split = split;
        _cursor = _split.getCursor();
    }
	
	public void close() throws IOException { }

	@Override
	public TypedBytesWritable createKey() {
		return new TypedBytesWritable();
	}

	@Override
	public TypedBytesWritable createValue() {
		return new TypedBytesWritable();
	}
	
	public byte[] getCurrentKey() {
		String test = _cur.get("_id").toString();
		return test.getBytes();
	}
	
	public byte[] getCurrentValue() {
        //return _cur;
		String test = _cur.toString();
		return test.getBytes();
    }
	
	@Override
	public float getProgress() throws IOException {
		return _seen / _total;
	}

	@Override
	public long getPos() throws IOException {
		return new Float(_seen).longValue();
	}
	
	public void initialize(InputSplit split, TaskAttemptContext context) {
        if (split != _split) throw new IllegalStateException("split != _split ??? ");
        _total = _cursor.size();
    }
	
	public boolean nextKeyValue() {
        if (!_cursor.hasNext()) return false;
        _cur = _cursor.next();
        _seen++;
        return true;
    }

	@Override
	public boolean next(TypedBytesWritable key, TypedBytesWritable value) throws IOException {
		
		if (nextKeyValue()) {
            if (_cur != null) {
            	key.setValue(new Buffer(getCurrentKey()));
                value.setValue(new Buffer(getCurrentValue()));
            	//log.info( _cur.get("_id") );
            } else {
            	log.warn("_CUR IS NULL!!!");
            }
            return true;
        }
        else {
            log.info("Cursor exhausted.");
            return false;
        }
	}
	
	final MongoInputSplit _split;
    final DBCursor _cursor;

    BSONObject _cur;
    float _seen = 0;
    float _total;

    private static final Log log = LogFactory.getLog(MongoRecordReader.class);
 
}
