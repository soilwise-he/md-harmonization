# Process harvested records

-	Select new records form harvest items table
-	See if it is a duplicate
    - update, merge or flag conflict
    - update (alt)identifiers
-	Harmonise records to schema/db
    - Element matcher
    - Dublin core/iso/dcat to schema.org
    - load schema.org to db




## About the properties

### title, abstract, subject

Common search parameters

### type

the type of resource

### temporal begin-end vs creation/modification date vs datestamp

- temporal begin - end is the actual dates when the data has been collected/observed/predicted for
- date of creation/modification/publication are the dates the resource has been altered
- datestamp is the date the metadata record has been last updated

### spatial vs spatial_desc

- spatial has the box coordinates of an area which applies to the resource
- spatial_desc is a description for that location (city, region, country)

### scale/denominator vs resolution/denominator

Use scale/distance when:

- resolution is known in ground units

- data is raster or sensor-based

Use resolution/denominator when:

- data was compiled or intended for a specific map scale

- resolution cannot be meaningfully expressed as a distance

Providing both is allowed only if:

- the dataset genuinely supports both interpretations
(e.g. a raster product officially produced for a given cartographic scale)