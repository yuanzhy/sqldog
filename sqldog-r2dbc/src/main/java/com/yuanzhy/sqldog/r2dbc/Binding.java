package com.yuanzhy.sqldog.r2dbc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.yuanzhy.sqldog.core.util.Asserts;

import reactor.util.Logger;
import reactor.util.Loggers;

/**
 *
 */
public final class Binding {

    private static final Logger LOGGER = Loggers.getLogger(Binding.class);

    public static final Binding EMPTY = new Binding(0);

    private final int expectedSize;

    private final List<Object> parameters;

    /**
     * Create a new instance.
     *
     * @param expectedSize the expected number of parameters
     */
    public Binding(int expectedSize) {
        this.expectedSize = expectedSize;
        this.parameters = new ArrayList<>(Arrays.asList(new Object[expectedSize]));
    }

    /**
     * Add a parameter to the binding.
     *
     * @param index     the index of the parameter
     * @param parameter the parameter
     * @return this {@link Binding}
     * @throws IllegalArgumentException if {@code index} or {@code parameter} is {@code null}
     */
    public Binding add(int index, Object parameter) {
        Asserts.notNull(parameter, "parameter must not be null");
        if (index >= this.expectedSize) {
            throw new IndexOutOfBoundsException(String.format("Binding index %d when only %d parameters are expected", index, this.expectedSize));
        }
        this.parameters.set(index, parameter);
        return this;
    }

    /**
     * Clear/release binding values.
     */
    public void clear() {
        this.parameters.clear();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Binding that = (Binding) o;
        return Objects.equals(this.parameters, that.parameters);
    }

    /**
     * Returns the formats of the parameters in the binding.
     *
     * @return the formats of the parameters in the binding
     */

//    public List<Format> getParameterFormats() {
//        return getTransformedParameters(EncodedParameter::getFormat);
//    }

    /**
     * Returns the types of the parameters in the binding.
     *
     * @return the types of the parameters in the binding
     */
//    public int[] getParameterTypes() {
//
//        for (int i = 0; i < this.parameters.size(); i++) {
//            EncodedParameter parameter = this.parameters.get(i);
//            if (parameter == UNSPECIFIED) {
//                throw new IllegalStateException(String.format("No parameter specified for index %d", i));
//            }
//        }
//        return this.types;
//    }

    /**
     * Returns the values of the parameters in the binding.
     *
     * @return the values of the parameters in the binding
     */
    public List<Object> getParameterValues() {
        return this.parameters;
    }
//
//    Flux<Publisher<? extends ByteBuf>> parameterValues() {
//        return Flux.fromIterable(this.parameters).map(EncodedParameter::getValue);
//    }

    @Override
    public int hashCode() {
        return Objects.hash(this.parameters);
    }

    public boolean isEmpty() {
        return this.parameters.isEmpty();
    }

    public int size() {
        return this.parameters.size();
    }

    @Override
    public String toString() {
        return "Binding{" +
            "parameters=" + this.parameters +
            '}';
    }

    /**
     * Validates that the correct number of parameters have been bound.
     *
     * @throws IllegalStateException if the incorrect number of parameters have been bound
     */
    public void validate() {
        Asserts.isTrue(this.parameters.size() == this.expectedSize, "Bound parameter count does not match parameters in SQL statement");
    }

//    private <T> List<T> getTransformedParameters(Function<EncodedParameter, T> transformer) {
//
//        if (this.parameters.isEmpty()) {
//            return Collections.emptyList();
//        }
//
//        List<T> transformed = null;
//
//        for (int i = 0; i < this.parameters.size(); i++) {
//            EncodedParameter parameter = this.parameters.get(i);
//            if (parameter == UNSPECIFIED) {
//                throw new IllegalStateException(String.format("No parameter specified for index %d", i));
//            }
//
//            if (transformed == null) {
//                if (this.parameters.size() == 1) {
//                    return Collections.singletonList(transformer.apply(parameter));
//                }
//
//                transformed = new ArrayList<>(this.parameters.size());
//            }
//
//            transformed.add(transformer.apply(parameter));
//        }
//
//        return transformed;
//    }

}
