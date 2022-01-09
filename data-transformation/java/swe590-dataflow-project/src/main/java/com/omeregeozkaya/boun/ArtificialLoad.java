package com.omeregeozkaya.boun;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

class ArtificialLoad<T> extends PTransform<PCollection<T>, PCollection<T>> {

    static <T> ArtificialLoad<T> create() {
        return new ArtificialLoad<>();
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
        Coder<T> coder = input.getCoder();
        PCollection<T> pCollection = input.apply(
            MapElements.via(
                new SimpleFunction<T, T>() {
                    @Override
                    public T apply(T input) {
                        long targetTime = System.currentTimeMillis() + 1000 * 2;
                        int x = 0;
                        while (System.currentTimeMillis() < targetTime) {
                            x = x + (x % 2 == 0 ? -1 : 1);
                        }
                        return input;
                    }
                }
            )
        ).setCoder(coder);
        return pCollection;
    }
}
