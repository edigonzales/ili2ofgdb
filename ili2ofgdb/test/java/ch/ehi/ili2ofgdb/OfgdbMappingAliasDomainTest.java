package ch.ehi.ili2ofgdb;

import ch.ehi.ili2db.gui.Config;
import ch.ehi.sqlgen.repository.DbColVarchar;
import ch.ehi.sqlgen.repository.DbTable;
import ch.ehi.sqlgen.repository.DbTableName;
import ch.interlis.ili2c.config.Configuration;
import ch.interlis.ili2c.config.FileEntry;
import ch.interlis.ili2c.config.FileEntryKind;
import ch.interlis.ili2c.metamodel.AttributeDef;
import ch.interlis.ili2c.metamodel.TransferDescription;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class OfgdbMappingAliasDomainTest {

    @Test
    public void fixupAttributeKeepsEnumAliasAndSkipsBooleanAlias() throws Exception {
        TransferDescription td = compileModel("test/data/Enum23/Enum23.ili");

        Config config = new Config();
        new OfgdbMain().initConfig(config);
        config.setMaxSqlNameLength("60");
        config.setFgdbCreateDomains(true);
        config.setTransientObject(Config.TRANSIENT_MODEL, td);

        OfgdbMapping mapping = new OfgdbMapping();
        mapping.fromIliInit(config);

        AttributeDef enumAliasAttr = (AttributeDef) td.getElement("Enum23.TestA.ClassA1.attr2");
        AttributeDef boolAliasAttr = (AttributeDef) td.getElement("Enum23.TestA.ClassA1.attr5");
        assertNotNull(enumAliasAttr);
        assertNotNull(boolAliasAttr);

        DbTable table = new DbTable();
        table.setName(new DbTableName(null, "c"));

        DbColVarchar enumColumn = new DbColVarchar();
        enumColumn.setName("enumattr");
        DbColVarchar boolColumn = new DbColVarchar();
        boolColumn.setName("boolattr");

        mapping.fixupAttribute(table, enumColumn, enumAliasAttr);
        mapping.fixupAttribute(table, boolColumn, boolAliasAttr);

        Map<String, ?> domains = readMapField(mapping, "domains");
        Map<String, ?> assignments = readMapField(mapping, "assignments");

        String enumDomainName = "Enum23_Enum1";
        String boolDomainName = "Enum23_BooleanDomain";

        assertTrue(domains.containsKey(enumDomainName));
        assertFalse(domains.containsKey(boolDomainName));

        assertTrue(assignments.containsKey("c.enumattr"));
        assertFalse(assignments.containsKey("c.boolattr"));

        Object enumDomainDefinition = domains.get(enumDomainName);
        Map<String, String> enumValues = readMapField(enumDomainDefinition, "codedValues");
        assertTrue(enumValues.containsKey("Test1"));
        assertTrue(enumValues.containsKey("Test2_ele"));
    }

    private static TransferDescription compileModel(String modelPath) throws Exception {
        Configuration ili2cConfig = new Configuration();
        ili2cConfig.addFileEntry(new FileEntry(modelPath, FileEntryKind.ILIMODELFILE));
        TransferDescription td = ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
        assertNotNull(td);
        return td;
    }

    @SuppressWarnings("unchecked")
    private static <T> Map<String, T> readMapField(Object instance, String fieldName) throws Exception {
        Field field = instance.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (Map<String, T>) field.get(instance);
    }
}
