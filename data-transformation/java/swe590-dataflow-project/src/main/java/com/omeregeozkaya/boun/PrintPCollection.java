package com.omeregeozkaya.boun;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

class PrintPCollection<T> extends PTransform<PCollection<T>, PCollection<T>> {

    static <T> PrintPCollection<T> create() {
        return new PrintPCollection<>();
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
        Coder<T> coder = input.getCoder();
        PCollection<T> pCollection = input.apply(
            MapElements.via(
                new SimpleFunction<T, T>() {
                    @Override
                    public T apply(T input) {
                        System.out.println(input);
                        return input;
                    }
                }
            )
        ).setCoder(coder);
        return pCollection;
    }
}
