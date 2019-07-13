package pl.edu.agh.csg;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class CloudletDescriptorTest {

    private final Gson gson = new Gson();

    @Test
    public void serializesDeserializesToJSON() {
        final CloudletDescriptor cloudletDescriptor = new CloudletDescriptor(1, 2, 3, 4);

        final String asJson = gson.toJson(cloudletDescriptor);
        final CloudletDescriptor deserialized = gson.fromJson(asJson, CloudletDescriptor.class);

        assertEquals(cloudletDescriptor, deserialized);
    }

    @Test
    public void serializesDeserializesListToJSON() {
        final CloudletDescriptor cloudletDescriptor1 = new CloudletDescriptor(1, 2, 3, 4);
        final CloudletDescriptor cloudletDescriptor5 = new CloudletDescriptor(5, 6, 7, 8);

        final List<CloudletDescriptor> cloudletDescriptors = Arrays.asList(cloudletDescriptor1, cloudletDescriptor5);

        final String serialized = gson.toJson(cloudletDescriptors);

        Type listType = new TypeToken<List<CloudletDescriptor>>(){}.getType();

        final List<CloudletDescriptor> deserialized = gson.fromJson(serialized, listType);

        System.out.println(deserialized);

        assertEquals(2, deserialized.size());
    }
}