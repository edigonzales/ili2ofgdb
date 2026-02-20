

## createSchema Statements

generelle Parametrisierung:

```bash
java -jar /Users/stefan/Downloads/ili2ofgdb-5.5.2-20260220.190822-2-bindist/ili2ofgdb-5.5.2-SNAPSHOT.jar --schemaimport --defaultSrsCode 2056 --fgdbXyResolution 0.0001 --fgdbXyTolerance 0.0001 --smart1Inheritance  --coalesceMultiSurface --coalesceMultiLine --coalesceMultiPoint --expandMultilingual --createEnumTxtCol --createEnumTabs --beautifyEnumDispName --createStdCols --createTidCol --createBasketCol --createDatasetCol --createFk --createFkIdx --createUnique --createNumChecks --createTextChecks --createDateTimeChecks --createMandatoryChecks --createTypeConstraint --dbfile DMAV_Bodenbedeckung_V1_1.gdb --log DMAV_Bodenbedeckung_V1_1.log --logtime --trace --models DMAV_Bodenbedeckung_V1_1
```

```bash
java -jar /Users/stefan/Downloads/ili2gpkg-5.5.2-20260102.100144-2-bindist/ili2gpkg-5.5.2-SNAPSHOT.jar --schemaimport --defaultSrsCode 2056  --smart1Inheritance  --coalesceMultiSurface --coalesceMultiLine --coalesceMultiPoint --expandMultilingual --createEnumTxtCol --createEnumTabs --beautifyEnumDispName --createStdCols --createTidCol --createBasketCol --createDatasetCol --createFk --createFkIdx --createUnique --createNumChecks --createTextChecks --createDateTimeChecks --createMandatoryChecks --createTypeConstraint --dbfile DMAV_Bodenbedeckung_V1_1.gpkg --log DMAV_Bodenbedeckung_V1_1.log --logtime --trace --models DMAV_Bodenbedeckung_V1_1
```






java -jar /Users/stefan/Downloads/ili2ofgdb-5.5.2-20260220.190822-2-bindist/ili2ofgdb-5.5.2-SNAPSHOT.jar --schemaimport --defaultSrsCode 2056 --fgdbXyResolution 0.0001 --fgdbXyTolerance 0.0001 --smart1Inheritance  --coalesceMultiSurface --coalesceMultiLine --coalesceMultiPoint --expandMultilingual --createEnumTxtCol --createEnumTabs --beautifyEnumDispName --createStdCols --createTidCol --createBasketCol --createDatasetCol --createFk --createFkIdx --createUnique --createNumChecks --createTextChecks --createDateTimeChecks --createMandatoryChecks --createTypeConstraint --dbfile SO_AFU_ABBAUSTELLEN_20210630.gdb --log SO_AFU_ABBAUSTELLEN_20210630.log --logtime --trace --models SO_AFU_ABBAUSTELLEN_20210630