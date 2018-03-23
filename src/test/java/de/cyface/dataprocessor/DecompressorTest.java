package de.cyface.dataprocessor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import de.cyface.dataprocessor.CyfaceCompressedDataProcessor.CyfaceCompressedDataProcessorException;

public class DecompressorTest {

    FileInputStream fileInputStream;

    @Before
    public void setUp() throws IOException {
        fileInputStream = new FileInputStream(this.getClass().getResource("/compressedCyfaceData").getFile());
    }

    @Test
    public void testDecompressCyfaceBinary() throws CyfaceCompressedDataProcessorException, IOException {
        CyfaceCompressedDataProcessor proc = null;
        try {
            proc = new CyfaceCompressedDataProcessor(fileInputStream);
            proc.decompress();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        byte[] individualBytes = proc.getDecompressedBinaryAsArray();
        assertEquals(individualBytes.length, 116398);

        ByteBuffer buffer = ByteBuffer.wrap(individualBytes);
        short formatVersion = buffer.order(ByteOrder.BIG_ENDIAN).getShort(0);
        assertThat(formatVersion, is(equalTo((short)1)));
        int numberOfGeoLocations = buffer.order(ByteOrder.BIG_ENDIAN).getInt(2);
        assertThat(numberOfGeoLocations, is(equalTo(1711)));
        int numberOfAccelerations = buffer.order(ByteOrder.BIG_ENDIAN).getInt(6);
        assertThat(numberOfAccelerations, is(equalTo(678)));
        int numberOfRotations = buffer.order(ByteOrder.BIG_ENDIAN).getInt(10);
        assertThat(numberOfRotations, is(equalTo(1032)));
        int numberOfDirections = buffer.order(ByteOrder.BIG_ENDIAN).getInt(14);
        assertThat(numberOfDirections, is(equalTo(2)));

        List<Map<String, ?>> geoLocs = proc.getLocationDataAsList(0);
        geoLocs.forEach(item -> {
            System.out.println(item.toString());
        });
    }
}
