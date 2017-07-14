package com.kmwllc.brigade.stage.dict;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by matt on 4/20/17.
 */
public class FSTDictionaryManager implements DictionaryManager {

    private static final String sep = ",";
    private FST<BytesRef> fst;

    @Override
    public void loadDictionary(InputStream in) {
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String s;
        try {
            while ((s = br.readLine()) != null) {
                String[] ss = s.split(sep);
                Set<String> payloads = new HashSet<>();
                if (ss.length > 1) {
                    for (int i = 1; i < ss.length; i++) {
                        payloads.add(ss[i]);
                    }
                }
                String p = String.join(sep, payloads);

                Builder<BytesRef> b = new Builder<>(FST.INPUT_TYPE.BYTE1, ByteSequenceOutputs.getSingleton());

                b.add(toIntsRef(new BytesRef(ss[0])), new BytesRef(p));

                fst = b.finish();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
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

    private <T> boolean hasPrefix(FST<T> fst, BytesRef input) throws IOException {
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

    @Override
    public boolean hasTokens(List<String> t) {
        String key = String.join(" ", t);
        try {
            return hasPrefix(fst, new BytesRef(key));
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public EntityInfo getEntity(List<String> t) {
        String key = String.join(" ", t);
        try {
            BytesRef out = Util.get(fst, new BytesRef(key));
            EntityInfo ei = new EntityInfo();
            ei.setTerm(key);
            ei.setPayloads(Arrays.asList(out.utf8ToString().split(sep)));
            return ei;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}