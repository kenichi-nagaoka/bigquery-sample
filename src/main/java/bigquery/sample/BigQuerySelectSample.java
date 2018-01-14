package bigquery.sample;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;

public class BigQuerySelectSample {

	private static final String DATASET_NAME = "dev1";
	private static final String TABLE_NAME = "names_2014";
	private static final int NAME_COLUMN = 0;
	private static final int GENDER_COLUMN = 1;

	public static void main(String[] args) throws InterruptedException {

		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

		// use listTableData method
		Page<FieldValueList> tableData = bigquery.listTableData(DATASET_NAME, TABLE_NAME);
		for (FieldValueList row : tableData.iterateAll()) {
			System.out.println(row.get(NAME_COLUMN).getValue() + " " + row.get(GENDER_COLUMN).getValue());
		}

		// use query method
		String query = "SELECT name, gender FROM `" + DATASET_NAME + "." + TABLE_NAME + "` WHERE name = 'Ryder'";
		QueryJobConfiguration queryConfig = QueryJobConfiguration.of(query);
		QueryResponse response = bigquery.query(queryConfig);
		for (FieldValueList row : response.getResult().iterateAll()) {
			System.out.println(row.get("name").getValue() + " " + row.get("gender").getValue());
		}
	}
}