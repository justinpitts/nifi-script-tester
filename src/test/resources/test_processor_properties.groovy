import org.apache.nifi.processor.io.OutputStreamCallback

flowFile = session.get()
if(!flowFile) return
session.write(flowFile, {output ->
    output.withPrintWriter { o ->
        o.println(processorproperty)
    }
} as OutputStreamCallback)
session.transfer(flowFile, REL_SUCCESS)