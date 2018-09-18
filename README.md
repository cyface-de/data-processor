# data-processor
This is the library for serializing/deserializing as well as compressing/decompressing the Cyface binary format.

## Usage
-------------------

1. Create a CyfaceDataProcessor object for each cyface binary you want to process as `InputStream` and tell the processor, if the source binary is `compressed`. Two implemenations for either in-memory or filesystem processing are available for usage:

    * `CyfaceDataProcessor proc = new CyfaceDataProcessorOnDiskImpl(binInputStream, compressed);`
    * `CyfaceDataProcessor proc = new CyfaceDataProcessorInMemoryImpl(binInputStream, compressed);`
    
2. Let the CyfaceDataProcessor uncompress and prepare the binary source for later data readout
`proc.uncompressAndPrepare();`

3. Read out data. Each step is **optional** 
   1. Read out *header info* with further readout options for instance for format version and number of data points for each sensor
   `CyfaceBinaryHeader header = proc.getHeader();` 
   2. Read out *sensor data*. Poll methods will return `null` if no further point is available for a specific sensor. Check point types `LocationPoint` and `Point3D` for specific value read out options.
      * `LocationPoint locPoint = proc.pollNextLocationPoint();`
      * `Point3D accPoint = proc.pollNextAccelerationPoint();`
      * `Point3D rotPoint = proc.pollNextRotationPoint();`
      * `Point3D dirPoint = proc.pollNextDirectionPoint();`
   **Hint**: Point3D.toString() prints out human readable sensor values.
   
4. After complete read out, **don't forget** to close the processor to release resources!
`proc.close();`
