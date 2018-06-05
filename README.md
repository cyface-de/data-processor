# data-processor
This is the library for serializing/deserializing as well as compressing/decompressing the Cyface binary format.

## Usage
-------------------

1. Create a CyfaceDataProcessor object for each cyface binary as InputStream, and tell the Processor if the source binary is compressed. 
`CyfaceDataProcessor proc = new CyfaceDataProcessor(binInputStream, compressedBoolean);`

2. Let the CyfaceDataProcessor uncompress and prepare the binary source for later data readout
`proc.uncompressAndPrepare();`

3. Read out data. Each step is **optional** 
   1. read out *header info* with further readout options for instance for format version and number of data points for each sensor
   `CyfaceBinaryHeader header = proc.getHeader();` 
   2. read out *sensor data*
      * `LocationPoint locPoint = proc.pollNextLocationPoint();`
      * `Point3D accPoint = proc.pollNextAccelerationPoint();`
      * `Point3D rotPoint = proc.pollNextRotationPoint();`
      * `Point3D dirPoint = proc.pollNextDirectionPoint();`

4. After complete read out, **don't forget** to close the processor to release resources!
`proc.close();`