/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: enum.proto

package com.alipay.sofa.jraft.entity;

public final class EnumOutter {
    private EnumOutter() {
    }

    public static void registerAllExtensions(com.google.protobuf.ExtensionRegistryLite registry) {
    }

    public static void registerAllExtensions(com.google.protobuf.ExtensionRegistry registry) {
        registerAllExtensions((com.google.protobuf.ExtensionRegistryLite) registry);
    }

    /**
     * Protobuf enum {@code jraft.EntryType}
     */
    public enum EntryType implements com.google.protobuf.ProtocolMessageEnum {
        /**
         * <code>ENTRY_TYPE_UNKNOWN = 0;</code>
         */
        ENTRY_TYPE_UNKNOWN(0),
        /**
         * <code>ENTRY_TYPE_NO_OP = 1;</code>
         */
        ENTRY_TYPE_NO_OP(1),
        /**
         * <code>ENTRY_TYPE_DATA = 2;</code>
         */
        ENTRY_TYPE_DATA(2),
        /**
         * <code>ENTRY_TYPE_CONFIGURATION = 3;</code>
         */
        ENTRY_TYPE_CONFIGURATION(3),
        ;

        /**
         * <code>ENTRY_TYPE_UNKNOWN = 0;</code>
         */
        public static final int ENTRY_TYPE_UNKNOWN_VALUE = 0;
        /**
         * <code>ENTRY_TYPE_NO_OP = 1;</code>
         */
        public static final int ENTRY_TYPE_NO_OP_VALUE = 1;
        /**
         * <code>ENTRY_TYPE_DATA = 2;</code>
         */
        public static final int ENTRY_TYPE_DATA_VALUE = 2;
        /**
         * <code>ENTRY_TYPE_CONFIGURATION = 3;</code>
         */
        public static final int ENTRY_TYPE_CONFIGURATION_VALUE = 3;

        @Override
        public final int getNumber() {
            return value;
        }

        /**
         * @deprecated Use {@link #forNumber(int)} instead.
         */
        @java.lang.Deprecated
        public static EntryType valueOf(int value) {
            return forNumber(value);
        }

        public static EntryType forNumber(int value) {
            switch (value) {
                case 0:
                    return ENTRY_TYPE_UNKNOWN;
                case 1:
                    return ENTRY_TYPE_NO_OP;
                case 2:
                    return ENTRY_TYPE_DATA;
                case 3:
                    return ENTRY_TYPE_CONFIGURATION;
                default:
                    return null;
            }
        }

        public static com.google.protobuf.Internal.EnumLiteMap<EntryType> internalGetValueMap() {
            return internalValueMap;
        }

        private static final com.google.protobuf.Internal.EnumLiteMap<EntryType> internalValueMap = new com.google.protobuf.Internal.EnumLiteMap<EntryType>() {
            @Override
            public EntryType findValueByNumber(int number) {
                return EntryType
                        .forNumber(number);
            }
        };

        @Override
        public final com.google.protobuf.Descriptors.EnumValueDescriptor getValueDescriptor() {
            return getDescriptor().getValues().get(this.ordinal());
        }

        @Override
        public final com.google.protobuf.Descriptors.EnumDescriptor getDescriptorForType() {
            return getDescriptor();
        }

        public static final com.google.protobuf.Descriptors.EnumDescriptor getDescriptor() {
            return com.alipay.sofa.jraft.entity.EnumOutter.getDescriptor().getEnumTypes().get(0);
        }

        private static final EntryType[] VALUES = values();

        public static EntryType valueOf(com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
            if (desc.getType() != getDescriptor()) {
                throw new java.lang.IllegalArgumentException("EnumValueDescriptor is not for this type.");
            }
            return VALUES[desc.getIndex()];
        }

        private final int value;

        private EntryType(int value) {
            this.value = value;
        }

        // @@protoc_insertion_point(enum_scope:jraft.EntryType)
    }

    /**
     * Protobuf enum {@code jraft.ErrorType}
     */
    public enum ErrorType implements com.google.protobuf.ProtocolMessageEnum {
        /**
         * <code>ERROR_TYPE_NONE = 0;</code>
         */
        ERROR_TYPE_NONE(0),
        /**
         * <code>ERROR_TYPE_LOG = 1;</code>
         */
        ERROR_TYPE_LOG(1),
        /**
         * <code>ERROR_TYPE_STABLE = 2;</code>
         */
        ERROR_TYPE_STABLE(2),
        /**
         * <code>ERROR_TYPE_SNAPSHOT = 3;</code>
         */
        ERROR_TYPE_SNAPSHOT(3),
        /**
         * <code>ERROR_TYPE_STATE_MACHINE = 4;</code>
         */
        ERROR_TYPE_STATE_MACHINE(4),
        /**
         * <code>ERROR_TYPE_META = 5;</code>
         */
        ERROR_TYPE_META(5),
        ;

        /**
         * <code>ERROR_TYPE_NONE = 0;</code>
         */
        public static final int ERROR_TYPE_NONE_VALUE = 0;
        /**
         * <code>ERROR_TYPE_LOG = 1;</code>
         */
        public static final int ERROR_TYPE_LOG_VALUE = 1;
        /**
         * <code>ERROR_TYPE_STABLE = 2;</code>
         */
        public static final int ERROR_TYPE_STABLE_VALUE = 2;
        /**
         * <code>ERROR_TYPE_SNAPSHOT = 3;</code>
         */
        public static final int ERROR_TYPE_SNAPSHOT_VALUE = 3;
        /**
         * <code>ERROR_TYPE_STATE_MACHINE = 4;</code>
         */
        public static final int ERROR_TYPE_STATE_MACHINE_VALUE = 4;
        /**
         * <code>ERROR_TYPE_META = 5;</code>
         */
        public static final int ERROR_TYPE_META_VALUE = 5;

        @Override
        public final int getNumber() {
            return value;
        }

        /**
         * @deprecated Use {@link #forNumber(int)} instead.
         */
        @java.lang.Deprecated
        public static ErrorType valueOf(int value) {
            return forNumber(value);
        }

        public static ErrorType forNumber(int value) {
            switch (value) {
                case 0:
                    return ERROR_TYPE_NONE;
                case 1:
                    return ERROR_TYPE_LOG;
                case 2:
                    return ERROR_TYPE_STABLE;
                case 3:
                    return ERROR_TYPE_SNAPSHOT;
                case 4:
                    return ERROR_TYPE_STATE_MACHINE;
                case 5:
                    return ERROR_TYPE_META;
                default:
                    return null;
            }
        }

        public static com.google.protobuf.Internal.EnumLiteMap<ErrorType> internalGetValueMap() {
            return internalValueMap;
        }

        private static final com.google.protobuf.Internal.EnumLiteMap<ErrorType> internalValueMap = new com.google.protobuf.Internal.EnumLiteMap<ErrorType>() {
            @Override
            public ErrorType findValueByNumber(int number) {
                return ErrorType
                        .forNumber(number);
            }
        };

        @Override
        public final com.google.protobuf.Descriptors.EnumValueDescriptor getValueDescriptor() {
            return getDescriptor().getValues().get(this.ordinal());
        }

        @Override
        public final com.google.protobuf.Descriptors.EnumDescriptor getDescriptorForType() {
            return getDescriptor();
        }

        public static final com.google.protobuf.Descriptors.EnumDescriptor getDescriptor() {
            return com.alipay.sofa.jraft.entity.EnumOutter.getDescriptor().getEnumTypes().get(1);
        }

        private static final ErrorType[] VALUES = values();

        public static ErrorType valueOf(com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
            if (desc.getType() != getDescriptor()) {
                throw new java.lang.IllegalArgumentException("EnumValueDescriptor is not for this type.");
            }
            return VALUES[desc.getIndex()];
        }

        private final int value;

        private ErrorType(int value) {
            this.value = value;
        }

        // @@protoc_insertion_point(enum_scope:jraft.ErrorType)
    }

    public static com.google.protobuf.Descriptors.FileDescriptor getDescriptor() {
        return descriptor;
    }

    private static com.google.protobuf.Descriptors.FileDescriptor descriptor;

    static {
        java.lang.String[] descriptorData = {"\n\nenum.proto\022\005jraft*l\n\tEntryType\022\026\n\022ENTR"
                + "Y_TYPE_UNKNOWN\020\000\022\024\n\020ENTRY_TYPE_NO_OP\020\001\022\023"
                + "\n\017ENTRY_TYPE_DATA\020\002\022\034\n\030ENTRY_TYPE_CONFIG"
                + "URATION\020\003*\227\001\n\tErrorType\022\023\n\017ERROR_TYPE_NO"
                + "NE\020\000\022\022\n\016ERROR_TYPE_LOG\020\001\022\025\n\021ERROR_TYPE_S"
                + "TABLE\020\002\022\027\n\023ERROR_TYPE_SNAPSHOT\020\003\022\034\n\030ERRO"
                + "R_TYPE_STATE_MACHINE\020\004\022\023\n\017ERROR_TYPE_MET"
                + "A\020\005B*\n\034com.alipay.sofa.jraft.entityB\nEnu" + "mOutter"};
        com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner = new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
            @Override
            public com.google.protobuf.ExtensionRegistry assignDescriptors(com.google.protobuf.Descriptors.FileDescriptor root) {
                descriptor = root;
                return null;
            }
        };
        com.google.protobuf.Descriptors.FileDescriptor.internalBuildGeneratedFileFrom(descriptorData,
                new com.google.protobuf.Descriptors.FileDescriptor[] {}, assigner);
    }

    // @@protoc_insertion_point(outer_class_scope)
}
