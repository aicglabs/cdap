package com.continuuity.api.flow;

import com.continuuity.api.annotation.DataSet;
import com.continuuity.api.annotation.Process;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.flowlet.Flowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class provides specification of a Flow. Instance of this class should be created through
 * the {@link Builder} class by invoking the {@link #builder()} method.
 *
 * <pre>
 * {@code
 * FlowSpecification flowSpecification =
 *      FlowSpecification.builder()
 *        .setName("tokenCount")
 *        .setDescription("Token counting flow")
 *        .withDataset().add("token")
 *        .withStream().add("text")
 *        .withFlowlet().add("source", StreamSource.class, 1).apply()
 *                      .add("tokenizer", Tokenizer.class, 1).setCpu(1).setMemoryInMB(100).apply()
 *                      .add("count", CountByField.class, 1).apply()
 *        .withInput().add("text", "source")
 *        .withConnection().add("source", "tokenizer")
 *                         .add("tokenizer", "count")
 *        .build();
 * }
 * </pre>
 */
public final class FlowSpecification {
  private static final String PROCESS_METHOD_PREFIX = "process";
  private static final String DEFAULT_OUTPUT = "out";
  private static final String ANY_INPUT = "";

  /**
   * Name of the flow
   */
  private final String name;

  /**
   * Description about the flow
   */
  private final String description;

  /**
   * Set of flowlets that constitute the flow. Map from flowlet id to {@link FlowletDefinition}
   */
  private final Map<String, FlowletDefinition> flowlets;

  /**
   * Stores flowlet connections.
   */
  private final List<FlowletConnection> connections;

  /**
   * Creates a {@link Builder} for building instance of this class.
   *
   * @return a new builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Private constructor, only called by {@link Builder}.
   */
  private FlowSpecification(String name, String description,
                            Map<String, FlowletDefinition> flowlets,
                            List<FlowletConnection> connections) {
    this.name = name;
    this.description = description;
    this.flowlets = flowlets;
    this.connections = connections;
  }

  /**
   * @return Name of the flow.
   */
  public String getName() {
    return name;
  }

  /**
   * @return Description of the flow.
   */
  public String getDescription() {
    return description;
  }

  /**
   * @return Immutable Map from flowlet name to {@link FlowletDefinition}.
   */
  public Map<String, FlowletDefinition> getFlowlets() {
    return flowlets;
  }

  /**
   * Class defining the definition for a flowlet.
   */
  public static final class FlowletDefinition {
    private final FlowletSpecification flowletSpec;
    private final int instances;
    private final ResourceSpecification resourceSpec;
    private final Map<String, TypeToken<?>> inputs;
    private final Map<String, TypeToken<?>> outputs;

    private FlowletDefinition(Flowlet flowlet, int instances, ResourceSpecification resourceSpec) {
      this.flowletSpec = flowlet.configure();
      this.instances = instances;
      this.resourceSpec = resourceSpec;

      Map<String, TypeToken<?>> inputs = Maps.newHashMap();
      Map<String, TypeToken<?>> outputs = Maps.newHashMap();
      inspectFlowlet(flowlet.getClass(), inputs, outputs);

      this.inputs = Collections.unmodifiableMap(inputs);
      this.outputs = Collections.unmodifiableMap(outputs);
    }

    /**
     * @return Number of instances configured for this flowlet.
     */
    public int getInstances() {
      return instances;
    }

    /**
     * @return Specification of Flowlet
     */
    public FlowletSpecification getFlowletSpec() {
      return flowletSpec;
    }

    /**
     * @return Specification for resource.
     */
    public ResourceSpecification getResourceSpec() {
      return resourceSpec;
    }

    /**
     * @return Mapping of name to the method types for processing inputs.
     */
    public Map<String, TypeToken<?>> getInputs() {
      return inputs;
    }

    /**
     * @return Mapping from name of {@link OutputEmitter} to actual emitters.
     */
    public Map<String, TypeToken<?>> getOutputs() {
      return outputs;
    }

    /**
     * This method is responsible for inspecting the flowlet class and inspecting to figure out what
     * method are used for processing input and what are used for emitting outputs.
     * @param flowletClass defining the flowlet that needs to be inspected.
     * @param inputs reference to map of name to input methods used for processing events on queues.
     * @param outputs reference to map of name to {@link OutputEmitter} and the types they handle.
     */
    private void inspectFlowlet(Class<?> flowletClass,
                                Map<String, TypeToken<?>> inputs,
                                Map<String, TypeToken<?>> outputs) {
      TypeToken<?> flowletType = TypeToken.of(flowletClass);

      // Walk up the hierarchy of flowlet class.
      for (TypeToken<?> type : flowletType.getTypes().classes()) {

        // Grab all the OutputEmitter fields
        for (Field field : type.getRawType().getDeclaredFields()) {
          if (!OutputEmitter.class.equals(field.getType())) {
            continue;
          }

          Type emitterType = field.getGenericType();
          Preconditions.checkArgument(emitterType instanceof ParameterizedType,
                                      "Type info missing from OutputEmitter; class: %s; field: %s.", type, field);

          // Extract the Output type from the first type argument of OutputEmitter
          TypeToken<?> outputType = TypeToken.of(((ParameterizedType) emitterType).getActualTypeArguments()[0]);
          String outputName = field.isAnnotationPresent(DataSet.class) ?
                                  field.getAnnotation(DataSet.class).value() : DEFAULT_OUTPUT;
          if (!outputs.containsKey(outputName)) {
            outputs.put(outputName, outputType);
          }
        }

        // Grab all process methods
        for (Method method : type.getRawType().getDeclaredMethods()) {
          Process processAnnotation = method.getAnnotation(Process.class);
          if (!method.getName().startsWith(PROCESS_METHOD_PREFIX) && processAnnotation == null) {
            continue;
          }

          Type[] methodParams = method.getGenericParameterTypes();
          Preconditions.checkArgument(methodParams.length > 0,
                                      "Type parameter missing from process method; class: %s, method: %s", type, method);

          // Extract the Input type from the first parameter of the process method
          TypeToken<?> inputType = type.resolveType(methodParams[0]);
          if (processAnnotation == null || processAnnotation.value().length == 0) {
            inputs.put(ANY_INPUT, inputType);
          } else {
            for (String name : processAnnotation.value()) {
              inputs.put(name, inputType);
            }
          }
        }
      }
    }
  }

  /**
   * Class that defines a connection between two flowlets.
   */
  public static final class FlowletConnection {

    /**
     * Defines different types of source a flowlet can be connected to.
     */
    public enum SourceType {
      STREAM,
      FLOWLET
    }

    private final SourceType sourceType;
    private final String sourceName;
    private final String targetName;
    private final String sourceOutput;
    private final String targetInput;

    private FlowletConnection(SourceType sourceType, String sourceName, String targetName, String sourceOutput,
                             String targetInput) {
      this.sourceType = sourceType;
      this.sourceName = sourceName;
      this.targetName = targetName;
      this.sourceOutput = sourceOutput;
      this.targetInput = targetInput;
    }

    /**
     * @return Type of source.
     */
    public SourceType getSourceType() {
      return sourceType;
    }

    /**
     * @return name of the source.
     */
    public String getSourceName() {
      return sourceName;
    }

    /**
     * @return name of the flowlet, the connection is connected to.
     */
    public String getTargetName() {
      return targetName;
    }

    /**
     * @return Name of the output for the source.
     */
    public String getSourceOutput() {
      return sourceOutput;
    }

    /**
     * @return Name of the input for the connection target.
     */
    public String getTargetInput() {
      return targetInput;
    }
  }

  /**
   * Defines builder for building connections or topology for a flow.
   */
  public static final class Builder {
    private String name;
    private String description;
    private final Set<String> streams = Sets.newHashSet();
    private final Map<String, FlowletDefinition> flowlets = Maps.newHashMap();
    private final List<FlowletConnection> connections = Lists.newArrayList();

    /**
     * Sets the name of the Flow
     * @param name of the flow.
     * @return An instance of {@link DescriptionSetter}
     */
    public DescriptionSetter setName(String name) {
      Preconditions.checkArgument(name != null, "Name cannot be null.");
      this.name = name;
      return new DescriptionSetter();
    }

    /**
     * Defines a class for defining the actual description.
     */
    public final class DescriptionSetter {
      /**
       * Sets the description for the flow.
       * @param description of the flow.
       * @return A instance of {@link AfterDescription}
       */
      public AfterDescription setDescription(String description) {
        Preconditions.checkArgument(description != null, "Description cannot be null.");
        Builder.this.description = description;
        return new AfterDescription();
      }
    }

    /**
     * Defines a class that represents what needs to happen after a description
     * has been added.
     */
    public final class AfterDescription {
      /**
       * @return An instance of {@link FlowletAdder} for adding flowlets to specification.
       */
      public FlowletAdder withFlowlet() {
        return new MoreFlowlet();
      }
    }

    /**
     * FlowletAdder is responsible for capturing the information of a Flowlet during the
     * specification creation.
     */
    public interface FlowletAdder {
      /**
       * Add a flowlet to the flow.
       * @param flowlet to be added to flow.
       * @return An instance of {@link ResourceSpecification.Builder}
       */
      ResourceSpecification.Builder<MoreFlowlet> add(Flowlet flowlet);

      /**
       * Add a flowlet to flow with minimum number of instance to begin with.
       * @param flowlet to be added to flow.
       * @param instances of flowlet
       * @return An instance of {@link ResourceSpecification.Builder}
       */
      ResourceSpecification.Builder<MoreFlowlet> add(Flowlet flowlet, int instances);
    }

    /**
     * This class allows more flowlets to be defined. This is part of a controlled builder.
     */
    public final class MoreFlowlet implements FlowletAdder {

      /**
       * Add a flowlet to the flow.
       * @param flowlet to be added to flow.
       * @return An instance of {@link ResourceSpecification.Builder}
       */
      @Override
      public ResourceSpecification.Builder<MoreFlowlet> add(Flowlet flowlet) {
        return add(flowlet, 1);
      }

      /**
       * Adds a flowlet to flow with minimum number of instance to begin with.
       * @param flowlet to be added to flow.
       * @param instances of flowlet
       * @return An instance of {@link ResourceSpecification.Builder}
       */
      @Override
      public ResourceSpecification.Builder<MoreFlowlet> add(final Flowlet flowlet, final int instances) {
        Preconditions.checkArgument(flowlet != null, "Flowlet cannot be null");
        Preconditions.checkArgument(instances > 0, "Number of instances must be > 0");

        final MoreFlowlet moreFlowlet = this;
        return ResourceSpecification.<MoreFlowlet>builder(new Function<ResourceSpecification, MoreFlowlet>() {
          @Override
          public MoreFlowlet apply(ResourceSpecification resourceSpec) {
            FlowletDefinition flowletDef = new FlowletDefinition(flowlet, instances, resourceSpec);
            String flowletName = flowletDef.getFlowletSpec().getName();
            Preconditions.checkArgument(!flowlets.containsKey(flowletName), "Flowlet %s already defined", flowletName);
            flowlets.put(flowletName, flowletDef);
            return moreFlowlet;
          }
        });
      }

      /**
       * Defines a connection between two flowlets.
       * @return An instance of {@link ConnectFrom}
       */
      public ConnectFrom connect() {
        // Collect all input streams names
        for (FlowletDefinition flowletDef : flowlets.values()) {
          for (Map.Entry<String, TypeToken<?>> inputEntry : flowletDef.getInputs().entrySet()) {
            if (StreamEvent.class.equals(inputEntry.getValue().getRawType())) {
              streams.add(inputEntry.getKey());
            }
          }
        }
        return new Connector();
      }
    }

    /**
     * Defines the starting flowlet for a connection.
     */
    public interface ConnectFrom {
      /**
       * Defines the flowlet that is at start of the connection.
       * @param flowlet that is start of connection.
       * @return An instance of {@link ConnectTo} specifying the flowlet it will connect to.
       */
      ConnectTo from(Flowlet flowlet);

      /**
       * Defines the stream that the connection is reading from.
       * @param stream Instance of stream.
       * @return An instance of {@link ConnectTo} specifying the flowlet it will connect to.
       */
      ConnectTo from(Stream stream);
    }

    /**
     * Class defining the connect to interface for a connection.
     */
    public interface ConnectTo {
      /**
       * Defines the flowlet that the connection is connecting to.
       * @param flowlet the connection connects to.
       * @return A instance of {@link MoreConnect} to define more connections of flowlets in a flow.
       */
      MoreConnect to(Flowlet flowlet);
    }

    /**
     * Interface defines the building of FlowSpecification.
     */
    public interface MoreConnect extends ConnectFrom {
      /**
       * Constructs a {@link FlowSpecification}
       * @return An instance of {@link FlowSpecification}
       */
      FlowSpecification build();
    }

    /**
     * Class defines the connection between two flowlets.
     */
    public final class Connector implements ConnectFrom, ConnectTo, MoreConnect {

      private String fromStream;
      private FlowletDefinition fromFlowlet;

      /**
       * Defines the flowlet that is at start of the connection.
       * @param flowlet that is start of connection.
       * @return An instance of {@link ConnectTo} specifying the flowlet it will connect to.
       */
      @Override
      public ConnectTo from(Flowlet flowlet) {
        Preconditions.checkArgument(flowlet != null, "Flowlet cannot be null");
        String flowletName = flowlet.configure().getName();
        Preconditions.checkArgument(flowlets.containsKey(flowletName), "Undefined flowlet %s", flowletName);

        fromFlowlet = flowlets.get(flowletName);
        fromStream = null;

        return this;
      }

      /**
       * Defines the stream that the connection is reading from.
       * @param stream Instance of stream.
       * @return An instance of {@link ConnectTo} specifying the flowlet it will connect to.
       */
      @Override
      public ConnectTo from(Stream stream) {
        Preconditions.checkArgument(stream != null, "Stream cannot be null");
        StreamSpecification streamSpec = stream.configure();
        Preconditions.checkArgument(streams.contains(streamSpec.getName()) || streams.contains(ANY_INPUT),
                                    "Stream %s is not accepted by any configured flowlet.", streamSpec.getName());

        fromFlowlet = null;
        fromStream = streamSpec.getName();

        return this;
      }

      /**
       * Defines the flowlet that the connection is connecting to.
       * @param flowlet the connection connects to.
       * @return A instance of {@link MoreConnect} to define more connections of flowlets in a flow.
       */
      @Override
      public MoreConnect to(Flowlet flowlet) {
        Preconditions.checkArgument(flowlet != null, "Flowlet cannot be null");
        String flowletName = flowlet.configure().getName();
        Preconditions.checkArgument(flowlets.containsKey(flowletName), "Undefined flowlet %s", flowletName);

        FlowletDefinition flowletDef = flowlets.get(flowletName);

        if (fromStream != null) {
          TypeToken<?> type = flowletDef.getInputs().get(fromStream);
          String targetInput = fromStream;
          if (type == null) {
            type = flowletDef.getInputs().get(ANY_INPUT);
            targetInput = ANY_INPUT;
          }
          Preconditions.checkArgument(StreamEvent.class.equals(type.getRawType()), "Cannot cannot stream %s to " +
            "flowlet %s", fromStream, flowletName);
          connections.add(new FlowletConnection(FlowletConnection.SourceType.STREAM, "", flowletName, fromStream,
                                                targetInput));

        } else {
          // TODO: Check if the output types of fromFlowlet is compatible with input types of toFlowlet
          // Need supports from the serialization library to implement it.

          // connections.add(new FlowletConnection(FlowletConnection.SourceType.FLOWLET,
          // fromFlowlet.getFlowletSpec().getName(), flowletName, ))
        }

        return this;
      }

      @Override
      public FlowSpecification build() {
        return new FlowSpecification(name, description, Collections.unmodifiableMap(flowlets),
                                     Collections.unmodifiableList(connections));
      }
    }

    private Builder() {
    }
  }
}
