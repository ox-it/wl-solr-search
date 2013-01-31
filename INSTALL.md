### Sakai Properties

These properties already exist in the default implementation of Sakai Search:

- `search.tool.required`, set to *true* to enable indexing and search only on
sites with the search tool, *false* otherwise.
- `search.usersites.ignored`, set to *true* to enable indexing and search on
user sites, *false* otherwise.
- `search.enable`, set to *true* to enable Sakai Search on the server, *false*
otherwise.

These properties are specific to the Solr implementation:

- `search.service.impl`, bean name of the `SearchService` implementation.
Set it to `org.sakaiproject.search.solr.SolrSearchService` to use the Solr
implementation.
- `search.indexbuilder.impl`, bean name of the `SearchIndexBuilder`
implementation.
Set it to `org.sakaiproject.search.solr.SolrSearchIndexBuilder` to use the Solr
implementation.
- `search.solr.server`, url of the Solr instance.
*eg: http://localhost:8983/solr/sakai-search*

**Note:**
The properties used to select the implementation of Solr Search does not work
with the default implementation to this day.

### Solr

This implementation of Sakai Search works with solr in order to index and do
full text search.

It's recommended to use Solr 3.6.x with Solr-Cell

An example of configuration for Solr is available in the
`impl/src/main/test/Resources/solr/conf` directory.

####Solr-Cell

The [Solr-Cell](http://wiki.apache.org/solr/ExtractingRequestHandler) project is
a component allowing solr to work with [Apache Tika](http://tika.apache.org/) in
order to extract the content and metadata of different resources.

The default implementation of Sakai Search also relies on Apache Tika to obtain
the content of each document.

Solr-Cell isn't set up by default and needs some dependencies provided in the
`contrib/extraction` folder of the solr project.

####Request Handlers

In order to work properly, Sakai-Solr relies on a few functionalities of Solr.
Here are the URL used by Sakai-Solr and what is expected to be there

- `/admin/stats`, for `SolrInfoMBeanHandler`, to obtain statistics about
Solr, in particular the number of docs
pending.
- `/admin/ping`, for `PingRequestHandler`, to check if the Solr server
is alive
- `/spell`, for `SearchHandler`, returns only SpellCheck information
- `/search`, for `SearchHandler`, returns actual search results
- `/update`, for `XmlUpdateRequestHandler`, to insert new plain text entries.
- `/update/extract`, for `ExtractingRequestHandler` (aka Solr-Cell), to insert
new entries with an associated document.  

####Search Components

Some search components are used within Sakai-Solr to format or enhance the
result of a search.

- [Spellcheck Component](http://wiki.apache.org/solr/SpellCheckComponent)
allows to provide a "did you mean:" result for a search request based on the
content of indexed documents.
- [Term Vector Component](http://wiki.apache.org/solr/TermVectorComponent)
allows to obtain detailed information about which words are present in each
document and their frequency. It is used to provide a list of the most used
words in every result of a search.
- [Highlight Component](http://wiki.apache.org/solr/HighlightingParameters)
allows to highlight the parts of the result matching the search request.

####Solr Schema

A default schema is provided, but in most cases some customisation is required.

With the default configuration every additional properties on resources are stored as `property_propertyname`, for
example `property_creationdate`.
These properties are automatically handled by the dynamic field `property_*`, and by default are just ignored.

If one of these properties (say `creationdate`) should be indexed, adding a new field in the schema will be enough

    <field name="property_creationdate" type="date" stored="true" />

Sometimes having a property named `property_creationdate` isn't the best way to go. In that case it's possible to choose
a custom name for the property:

    <field name="creationdate" type="date" stored="true" />
    <copyField source="property_creationdate" dest="creationdate" />


Tika properties (document's metadata) will behave the same way but will be stored in `property_tika_*` instead (to avoid
collisions).
