from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties
from azure.kusto.data.response import KustoResultTable
from datetime import timedelta
import json
import os

class KustoClusterManager:
    """Manages connections and operations for multiple Kusto clusters"""
    
    def __init__(self):
        self.clusters = {}
        self.cluster_urls = {
            "Shared": {
                "wabiasia": "https://wabiasia.kusto.windows.net",
                "wabiaus": "https://wabiaus.kusto.windows.net",
                "wabieu": "https://wabieu.kusto.windows.net",
                "wabinam": "https://wabinam.kusto.windows.net",
                "wabisam": "https://wabisam.kusto.windows.net",
                "wabiuk": "https://wabiuk.uksouth.kusto.windows.net",
                "wabiafrica": "https://wabiafrica.southafricanorth.kusto.windows.net"
            },
            "MWC": {
                "pbipam": "https://pbipam.kusto.windows.net",
                "pbipapac": "https://pbipapac.kusto.windows.net",
                "pbipeu": "https://pbipeu.kusto.windows.net",
                "pbipinternal": "https://pbipinternal.kusto.windows.net"
            }
        }
        self.databases = {
            "Shared": "biazurekustoprod",
            "MWC": "pbip"
        }
        self._initialize_clusters()
        self._setup_request_properties()

    def _setup_request_properties(self):
        """Initialize request properties for Kusto queries"""
        self.request_properties = ClientRequestProperties()
        self.request_properties.set_option("truncationmaxsize", 268435456)
        self.request_properties.set_option("truncationmaxrecords", 100000000)
        self.request_properties.set_option("servertimeout", timedelta(minutes=30))

    def _initialize_clusters(self):
        """Initialize connections to all Kusto clusters"""
        for cluster_type, clusters in self.cluster_urls.items():
            self.clusters[cluster_type] = {}
            for cluster_name, url in clusters.items():
                self.clusters[cluster_type][cluster_name] = self._create_client(url)

    def _create_client(self, cluster_url):
        """Create a new Kusto client with authentication"""
        return KustoClient(
            KustoConnectionStringBuilder.with_az_cli_authentication(cluster_url)
        )

    def execute_query(self, cluster_type, cluster_name, query):
        """Execute a query on a specific cluster"""
        try:
            client = self.clusters[cluster_type][cluster_name]
            db = self.databases[cluster_type]
            result = client.execute(db, query, self.request_properties)
            print(f"Execution done, result row number: {len(result.primary_results[0])}")
            return result.primary_results[0]
        except Exception as e:
            print(f"Error executing query: {str(e)} for query: {query}")
            return None

    def get_shared_cluster_client(self, cluster_name):
        """Get a specific shared cluster client"""
        return self.clusters.get("Shared", {}).get(cluster_name)

    def get_database_for_cluster_type(self, cluster_type):
        """Get the database name for a cluster type"""
        return self.databases.get(cluster_type)

class CacheManager:
    """
    Handles all caching operations for Kusto query results.
    Provides methods to read from and write to cache files.
    Implements singleton pattern.
    """
    _instance = None

    def __new__(cls, cache_dir=".cache"):
        if cls._instance is None:
            cls._instance = super(CacheManager, cls).__new__(cls)
            cls._instance.cache_dir = os.path.join(os.getcwd(), cache_dir)
            cls._instance._ensure_cache_dir_exists()
        return cls._instance

    def _ensure_cache_dir_exists(self):
        """Ensure the cache directory exists."""
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

    def get_cache_path(self, filename):
        """Get the full path for a cache file."""
        return os.path.join(self.cache_dir, filename)

    def read_from_cache(self, filename):
        """
        Read data from a cache file.
        
        Args:
            filename (str): Name of the cache file
            
        Returns:
            dict/list: Cached data if available, None if not
        """
        cache_path = self.get_cache_path(filename)
        if os.path.exists(cache_path) and os.path.getsize(cache_path) > 0:
            try:
                with open(cache_path, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError as e:
                print(f"Error reading cache file {filename}: {e}")
                return None
        return None

    def write_to_cache(self, filename, data):
        """
        Write data to a cache file.
        
        Args:
            filename (str): Name of the cache file
            data (dict/list): Data to cache
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with open(self.get_cache_path(filename), 'w') as f:
                json.dump(data, f, indent=4)
            return True
        except IOError as e:
            print(f"Error writing to cache file {filename}: {e}")
            return False

def get_tenant_home_cluster_for_each_cluster():
    cache_manager = CacheManager()
    kusto_manager = KustoClusterManager()
    tenant_home_cluster = {}

    def create_kusto_query_to_get_tenant_home_cluster():
        return """
        Trace
        | where TIMESTAMP >= ago(1h)
        | where ActivityType == 'GSR2'
        | where EventText hasprefix 'Global service retrieve entity with keys:'
        | parse EventText with * "TenantId='" TenantObjectId:string "'" * "FixedCluster='https://" FixedCluster:string '-redirect' *
        | extend FixedCluster = toupper(FixedCluster)
        | summarize arg_max(TIMESTAMP, FixedCluster) by TenantObjectId
        """

    for cluster_name in kusto_manager.cluster_urls["Shared"]:
        cache_filename = f"tenant_home_cluster_{cluster_name}.json"
        temp_table = cache_manager.read_from_cache(cache_filename)
        
        if temp_table:
            for entry in temp_table:
                tenant_home_cluster[entry[0]] = entry[1]
            continue

        result = kusto_manager.execute_query(
            "Shared",
            cluster_name,
            create_kusto_query_to_get_tenant_home_cluster()
        )

        if result is None:
            print(f"Result for Shared {cluster_name} is None")
            continue

        # Write result to a temporary table
        temp_table = []
        for row in result:
            tenant_id = row['TenantObjectId']
            fixed_cluster = row['FixedCluster']
            temp_table.append((tenant_id, fixed_cluster))

        # Write the temporary table to cache
        cache_manager.write_to_cache(cache_filename, temp_table)

        # Update the tenant_home_cluster dictionary
        for entry in temp_table:
            tenant_home_cluster[entry[0]] = entry[1]

    return tenant_home_cluster

def get_cluster_name_by_kusto_url():
    """
    Retrieves and caches a mapping between tenant names and their corresponding Kusto cluster names.
    """
    def get_tenant_trace_query():
        return """
        Trace 
        | where TIMESTAMP > ago(1h)            
        | summarize by Tenant
        """

    cache_manager = CacheManager()
    kusto_manager = KustoClusterManager()
    cluster_name_mapping = cache_manager.read_from_cache("cluster_name_mapping.json")
    
    if cluster_name_mapping:
        return cluster_name_mapping

    cluster_name_mapping = {}
    for cluster_name in kusto_manager.cluster_urls["Shared"]:
        result = kusto_manager.execute_query("Shared", cluster_name, get_tenant_trace_query())

        if result is None:
            raise Exception("Error getting cluster name by kusto url")
        else:
            for row in result:
                cluster_name_mapping[row['Tenant']] = cluster_name

    cache_manager.write_to_cache("cluster_name_mapping.json", cluster_name_mapping)
    return cluster_name_mapping

def get_kusto_client_and_db_for_fixed_cluster(kusto_manager, kcluster):
    """Get the appropriate client and database for a fixed cluster"""
    client = kusto_manager.get_shared_cluster_client(kcluster)
    if client:
        return client, kusto_manager.get_database_for_cluster_type("Shared")
    
    print(f"Warning: No match found for kcluster: {kcluster}")
    return None, None

def chunk_list(items, chunk_size=3000):
    """Split a list into smaller chunks."""
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]

def execute_chunked_queries(items_by_cluster, cluster_mapping, query_generator, result_aggregator, chunk_size=3000):
    """
    Executes queries in chunks with custom query generation and result aggregation.
    
    Args:
        items_by_cluster: Dictionary mapping clusters to their items (can be list or list of lists)
        cluster_mapping: Dictionary mapping cluster names to their URLs
        query_generator: Function(chunk, cluster) that generates the query string
        result_aggregator: Function(results, row, cluster) that processes each result row
        chunk_size: Size of each chunk (default: 3000)
    
    Returns:
        Aggregated results
    """
    kusto_manager = KustoClusterManager()
    results = {}

    # Execute query for each cluster
    for cluster, items in items_by_cluster.items():
        try:
            kcluster = cluster_mapping[cluster]
        except KeyError:
            print(f"Warning: No match found for cluster: {cluster}")
            continue

        # Flatten items only if it's a list of lists
        cluster_items = items
        if items and isinstance(items[0], list):
            cluster_items = [item for sublist in items for item in sublist]

        # Break items into chunks
        chunks = chunk_list(cluster_items, chunk_size)
        print(f"Executing query for {cluster} with items size {len(cluster_items)} and {len(chunks)} chunks")

        # Process each chunk
        for chunk_counter, chunk in enumerate(chunks):
            print(f"Executing query for {cluster} chunk {chunk_counter + 1} of {len(chunks)}")
            
            # Generate and execute query
            query = query_generator(chunk, cluster)
            result = kusto_manager.execute_query("Shared", kcluster, query)

            if result is None:
                print(f"No results for {cluster} with query: {query}")
                continue

            # Process results using the aggregator
            for row in result:
                results = result_aggregator(results, row, cluster)

    return results

def execute_query_for_tenants_in_same_cluster(tenant_home_cluster, cluster_name_to_url_mapping):
    """Example usage of execute_chunked_queries for tenant queries with caching."""
    cache_manager = CacheManager()
    cached_results = cache_manager.read_from_cache("all_results.json")
    
    if cached_results:
        return cached_results
    
    # Group tenants by cluster
    tenants_by_cluster = {}
    for tenant_id, cluster in tenant_home_cluster.items():
        if cluster not in tenants_by_cluster:
            tenants_by_cluster[cluster] = []
        tenants_by_cluster[cluster].append(tenant_id)
    
    def generate_tenant_query(chunk, cluster):
        tenant_ids_str = ", ".join(f"'{tenant_id}'" for tenant_id in chunk)
        return f"""
        let tenantObjectId = dynamic([{tenant_ids_str}]);
        Evt5653745998362568492
        | where Tenant == '{cluster}'
        | where TIMESTAMP > ago(1d)
        | where viewName == 'IPowerBIDatabaseContext.Viewers.PR_TenantsView'
        | extend data = parse_json(eventText)
        | evaluate bag_unpack(data)
        | where Name in (tenantObjectId)
        | summarize arg_max(SnapshotTime, Id, Tenant) by tostring(Name)
        """

    def aggregate_tenant_results(results, row, cluster):
        """Aggregates results into a single flattened list per cluster."""
        if cluster not in results:
            results[cluster] = set()  # Using set to avoid duplicates
            
        if isinstance(results[cluster], list):
            results[cluster].append(row['Id'])
        else:
            results[cluster].add(row['Id'])
        return {k: list(v) for k, v in results.items()}  # Convert sets to lists

    results = execute_chunked_queries(
        items_by_cluster=tenants_by_cluster,
        cluster_mapping=cluster_name_to_url_mapping,
        query_generator=generate_tenant_query,
        result_aggregator=aggregate_tenant_results
    )
    
    cache_manager.write_to_cache("all_results.json", results)
    return results

def get_workspace_counts_by_tenant_id(tenant_ids_by_cluster, cluster_name_to_url_mapping):
    """Get workspace counts for each tenant using chunked queries."""
    cache_manager = CacheManager()
    cached_results = cache_manager.read_from_cache("workspace_counts_by_tenant_id.json")
    
    if cached_results:
        return cached_results

    def generate_workspace_query(chunk, cluster):
        tenant_ids_str = ", ".join(f"'{tenant_id}'" for tenant_id in chunk)
        return f"""
        let tenant_ids = dynamic([{tenant_ids_str}]);
        Evt5653745998362568492
        | where TIMESTAMP > ago(1d)
        | where viewName == 'IPowerBIDatabaseContext.Viewers.PR_FoldersView'
        | extend data = parse_json(eventText)
        | evaluate bag_unpack(data)
        | where Tenant == "{cluster}"
        | where TenantId in (tenant_ids)
        | summarize arg_max(TIMESTAMP, *) by ObjectId, TenantId
        | summarize dcount(ObjectId) by TenantId
        """

    def aggregate_workspace_results(results, row, cluster):
        tenant_id = row['TenantId']
        count = row['dcount_ObjectId']
        if tenant_id not in results:
            results[tenant_id] = count
        else:
            print(f"Warning: Duplicate tenant_id: {tenant_id}")
        return results

    results = execute_chunked_queries(
        items_by_cluster=tenant_ids_by_cluster,
        cluster_mapping=cluster_name_to_url_mapping,
        query_generator=generate_workspace_query,
        result_aggregator=aggregate_workspace_results,
        chunk_size=4
    )
    
    cache_manager.write_to_cache("workspace_counts_by_tenant_id.json", results)
    return results


if __name__ == "__main__":

    #get cluster name by kusto url
    cluster_name_to_url_mapping = get_cluster_name_by_kusto_url()
    tenant_home_cluster = get_tenant_home_cluster_for_each_cluster()
    tenant_ids_by_cluster = execute_query_for_tenants_in_same_cluster(tenant_home_cluster, cluster_name_to_url_mapping)
    workspace_counts_by_tenant_id = get_workspace_counts_by_tenant_id(tenant_ids_by_cluster, cluster_name_to_url_mapping)

    for tenant_id, count in workspace_counts_by_tenant_id.items():
        print(f"Tenant {tenant_id} has {count} workspaces")
