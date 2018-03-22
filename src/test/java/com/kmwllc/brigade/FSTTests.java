package com.kmwllc.brigade;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by matt on 4/20/17.
 */
public class FSTTests {

    @Test
    public void testFST() {
        FST<BytesRef> fst = null;
        Map<String, String> map = new HashMap<>();
        map.put("Martin Luther", "German");
        map.put("Martin Luther King", "American");

        Builder<BytesRef> b = new Builder<>(FST.INPUT_TYPE.BYTE1, ByteSequenceOutputs.getSingleton());
        for (Map.Entry<String, String> e : map.entrySet()){
            String k = e.getKey();
            String v = e.getValue();
            try {
                b.add(toIntsRef(new BytesRef(k)), new BytesRef(v));
            } catch (IOException e1) {
                e1.printStackTrace();
                fail();
            }
        }
        try {
            fst = b.finish();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }

        try {
            BytesRef out = Util.get(fst, new BytesRef("Martin Luther"));
           assertEquals("German", out.utf8ToString());
            BytesRef out2 = Util.get(fst, new BytesRef("Martin Luther King"));
            assertEquals("American", out2.utf8ToString());
            BytesRef out3 = Util.get(fst, new BytesRef("Martin Lawrence"));
            assertEquals(null, out3);
            boolean hasPrefix = hasPrefix(fst, new BytesRef("Martin"));
            assertEquals(true, hasPrefix);
            boolean hasPrefix2 = hasPrefix(fst, new BytesRef("Margaret"));
            assertEquals(false, hasPrefix2);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }

    public static<T> boolean hasPrefix(FST<T> fst, BytesRef input) throws IOException {
        assert fst.inputType == FST.INPUT_TYPE.BYTE1;

        final FST.BytesReader fstReader = fst.getBytesReader();

        // TODO: would be nice not to alloc this on every lookup
        final FST.Arc<T> arc = fst.getFirstArc(new FST.Arc<T>());

        // Accumulate output as we go
        T output = fst.outputs.getNoOutput();
        for(int i=0;i<input.length;i++) {
            if (fst.findTargetArc(input.bytes[i+input.offset] & 0xFF, arc, arc, fstReader) == null) {
                return false;
            }
        }

        return true;
    }

    private IntsRef toIntsRef(BytesRef b) {
        IntsRefBuilder irb = new IntsRefBuilder();
        irb.grow(b.length);
        irb.clear();
        for (int i = 0; i < b.length; i++){
            irb.append(b.bytes[b.offset + i]&0xFF);
        }
        return irb.get();
    }
}
