// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: sdk/schema.proto

package com.alibaba.maxgraph.proto;

/**
 * Protobuf type {@code TypeOptionProto}
 */
public  final class TypeOptionProto extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:TypeOptionProto)
    TypeOptionProtoOrBuilder {
  // Use TypeOptionProto.newBuilder() to construct.
  private TypeOptionProto(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private TypeOptionProto() {
    storageEngine_ = 0;
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return com.google.protobuf.UnknownFieldSet.getDefaultInstance();
  }
  private TypeOptionProto(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    this();
    int mutable_bitField0_ = 0;
    try {
      boolean done = false;
      while (!done) {
        int tag = input.readTag();
        switch (tag) {
          case 0:
            done = true;
            break;
          default: {
            if (!input.skipField(tag)) {
              done = true;
            }
            break;
          }
          case 8: {
            int rawValue = input.readEnum();

            storageEngine_ = rawValue;
            break;
          }
        }
      }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw e.setUnfinishedMessage(this);
    } catch (java.io.IOException e) {
      throw new com.google.protobuf.InvalidProtocolBufferException(
          e).setUnfinishedMessage(this);
    } finally {
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return com.alibaba.maxgraph.proto.Schema.internal_static_TypeOptionProto_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return com.alibaba.maxgraph.proto.Schema.internal_static_TypeOptionProto_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            com.alibaba.maxgraph.proto.TypeOptionProto.class, com.alibaba.maxgraph.proto.TypeOptionProto.Builder.class);
  }

  public static final int STORAGEENGINE_FIELD_NUMBER = 1;
  private int storageEngine_;
  /**
   * <code>optional .StorageEngine storageEngine = 1;</code>
   */
  public int getStorageEngineValue() {
    return storageEngine_;
  }
  /**
   * <code>optional .StorageEngine storageEngine = 1;</code>
   */
  public com.alibaba.maxgraph.proto.StorageEngine getStorageEngine() {
    com.alibaba.maxgraph.proto.StorageEngine result = com.alibaba.maxgraph.proto.StorageEngine.valueOf(storageEngine_);
    return result == null ? com.alibaba.maxgraph.proto.StorageEngine.UNRECOGNIZED : result;
  }

  private byte memoizedIsInitialized = -1;
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    memoizedIsInitialized = 1;
    return true;
  }

  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (storageEngine_ != com.alibaba.maxgraph.proto.StorageEngine.MEMORY.getNumber()) {
      output.writeEnum(1, storageEngine_);
    }
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (storageEngine_ != com.alibaba.maxgraph.proto.StorageEngine.MEMORY.getNumber()) {
      size += com.google.protobuf.CodedOutputStream
        .computeEnumSize(1, storageEngine_);
    }
    memoizedSize = size;
    return size;
  }

  private static final long serialVersionUID = 0L;
  @java.lang.Override
  public boolean equals(final java.lang.Object obj) {
    if (obj == this) {
     return true;
    }
    if (!(obj instanceof com.alibaba.maxgraph.proto.TypeOptionProto)) {
      return super.equals(obj);
    }
    com.alibaba.maxgraph.proto.TypeOptionProto other = (com.alibaba.maxgraph.proto.TypeOptionProto) obj;

    boolean result = true;
    result = result && storageEngine_ == other.storageEngine_;
    return result;
  }

  @java.lang.Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptorForType().hashCode();
    hash = (37 * hash) + STORAGEENGINE_FIELD_NUMBER;
    hash = (53 * hash) + storageEngine_;
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static com.alibaba.maxgraph.proto.TypeOptionProto parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.alibaba.maxgraph.proto.TypeOptionProto parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.alibaba.maxgraph.proto.TypeOptionProto parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static com.alibaba.maxgraph.proto.TypeOptionProto parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static com.alibaba.maxgraph.proto.TypeOptionProto parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static com.alibaba.maxgraph.proto.TypeOptionProto parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static com.alibaba.maxgraph.proto.TypeOptionProto parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static com.alibaba.maxgraph.proto.TypeOptionProto parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static com.alibaba.maxgraph.proto.TypeOptionProto parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static com.alibaba.maxgraph.proto.TypeOptionProto parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(com.alibaba.maxgraph.proto.TypeOptionProto prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }
  public Builder toBuilder() {
    return this == DEFAULT_INSTANCE
        ? new Builder() : new Builder().mergeFrom(this);
  }

  @java.lang.Override
  protected Builder newBuilderForType(
      com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }
  /**
   * Protobuf type {@code TypeOptionProto}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:TypeOptionProto)
      com.alibaba.maxgraph.proto.TypeOptionProtoOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.alibaba.maxgraph.proto.Schema.internal_static_TypeOptionProto_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.alibaba.maxgraph.proto.Schema.internal_static_TypeOptionProto_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.alibaba.maxgraph.proto.TypeOptionProto.class, com.alibaba.maxgraph.proto.TypeOptionProto.Builder.class);
    }

    // Construct using com.alibaba.maxgraph.proto.TypeOptionProto.newBuilder()
    private Builder() {
      maybeForceBuilderInitialization();
    }

    private Builder(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      super(parent);
      maybeForceBuilderInitialization();
    }
    private void maybeForceBuilderInitialization() {
      if (com.google.protobuf.GeneratedMessageV3
              .alwaysUseFieldBuilders) {
      }
    }
    public Builder clear() {
      super.clear();
      storageEngine_ = 0;

      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return com.alibaba.maxgraph.proto.Schema.internal_static_TypeOptionProto_descriptor;
    }

    public com.alibaba.maxgraph.proto.TypeOptionProto getDefaultInstanceForType() {
      return com.alibaba.maxgraph.proto.TypeOptionProto.getDefaultInstance();
    }

    public com.alibaba.maxgraph.proto.TypeOptionProto build() {
      com.alibaba.maxgraph.proto.TypeOptionProto result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public com.alibaba.maxgraph.proto.TypeOptionProto buildPartial() {
      com.alibaba.maxgraph.proto.TypeOptionProto result = new com.alibaba.maxgraph.proto.TypeOptionProto(this);
      result.storageEngine_ = storageEngine_;
      onBuilt();
      return result;
    }

    public Builder clone() {
      return (Builder) super.clone();
    }
    public Builder setField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        Object value) {
      return (Builder) super.setField(field, value);
    }
    public Builder clearField(
        com.google.protobuf.Descriptors.FieldDescriptor field) {
      return (Builder) super.clearField(field);
    }
    public Builder clearOneof(
        com.google.protobuf.Descriptors.OneofDescriptor oneof) {
      return (Builder) super.clearOneof(oneof);
    }
    public Builder setRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        int index, Object value) {
      return (Builder) super.setRepeatedField(field, index, value);
    }
    public Builder addRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        Object value) {
      return (Builder) super.addRepeatedField(field, value);
    }
    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof com.alibaba.maxgraph.proto.TypeOptionProto) {
        return mergeFrom((com.alibaba.maxgraph.proto.TypeOptionProto)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(com.alibaba.maxgraph.proto.TypeOptionProto other) {
      if (other == com.alibaba.maxgraph.proto.TypeOptionProto.getDefaultInstance()) return this;
      if (other.storageEngine_ != 0) {
        setStorageEngineValue(other.getStorageEngineValue());
      }
      onChanged();
      return this;
    }

    public final boolean isInitialized() {
      return true;
    }

    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      com.alibaba.maxgraph.proto.TypeOptionProto parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (com.alibaba.maxgraph.proto.TypeOptionProto) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }

    private int storageEngine_ = 0;
    /**
     * <code>optional .StorageEngine storageEngine = 1;</code>
     */
    public int getStorageEngineValue() {
      return storageEngine_;
    }
    /**
     * <code>optional .StorageEngine storageEngine = 1;</code>
     */
    public Builder setStorageEngineValue(int value) {
      storageEngine_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional .StorageEngine storageEngine = 1;</code>
     */
    public com.alibaba.maxgraph.proto.StorageEngine getStorageEngine() {
      com.alibaba.maxgraph.proto.StorageEngine result = com.alibaba.maxgraph.proto.StorageEngine.valueOf(storageEngine_);
      return result == null ? com.alibaba.maxgraph.proto.StorageEngine.UNRECOGNIZED : result;
    }
    /**
     * <code>optional .StorageEngine storageEngine = 1;</code>
     */
    public Builder setStorageEngine(com.alibaba.maxgraph.proto.StorageEngine value) {
      if (value == null) {
        throw new NullPointerException();
      }
      
      storageEngine_ = value.getNumber();
      onChanged();
      return this;
    }
    /**
     * <code>optional .StorageEngine storageEngine = 1;</code>
     */
    public Builder clearStorageEngine() {
      
      storageEngine_ = 0;
      onChanged();
      return this;
    }
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return this;
    }

    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return this;
    }


    // @@protoc_insertion_point(builder_scope:TypeOptionProto)
  }

  // @@protoc_insertion_point(class_scope:TypeOptionProto)
  private static final com.alibaba.maxgraph.proto.TypeOptionProto DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new com.alibaba.maxgraph.proto.TypeOptionProto();
  }

  public static com.alibaba.maxgraph.proto.TypeOptionProto getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<TypeOptionProto>
      PARSER = new com.google.protobuf.AbstractParser<TypeOptionProto>() {
    public TypeOptionProto parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
        return new TypeOptionProto(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<TypeOptionProto> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<TypeOptionProto> getParserForType() {
    return PARSER;
  }

  public com.alibaba.maxgraph.proto.TypeOptionProto getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}
