# Module 10 - Real-time stream processing with Stream Analytics

In this module, students will learn how to process streaming data with Azure Stream Analytics. The student will ingest vehicle telemetry data into Event Hubs, then process that data in real time, using various windowing functions in Azure Stream Analytics. They will output the data to Azure Synapse Analytics. Finally, the student will learn how to scale the Stream Analytics job to increase throughput.

In this module, the student will be able to:

- Use Stream Analytics to process real-time data from Event Hubs
- Use Stream Analytics windowing functions to build aggregates and output to Synapse Analytics
- Scale the Azure Stream Analytics job to increase throughput through partitioning
- Repartition the stream input to optimize parallelization

## Lab details

- [Module 10 - Real-time stream processing with Stream Analytics](#module-10---real-time-stream-processing-with-stream-analytics)
  - [Lab details](#lab-details)
  - [Technology overview](#technology-overview)
    - [Azure Stream Analytics](#azure-stream-analytics)
    - [Azure Event Hubs](#azure-event-hubs)
  - [Scenario overview](#scenario-overview)
  - [Lab setup and pre-requisites](#lab-setup-and-pre-requisites)
  - [Exercise 0: Start the dedicated SQL pool](#exercise-0-start-the-dedicated-sql-pool)
  - [Exercise 1: Configure services](#exercise-1-configure-services)
    - [Task 1: Configure Event Hubs](#task-1-configure-event-hubs)
    - [Task 2: Configure Synapse Analytics](#task-2-configure-synapse-analytics)
    - [Task 3: Configure Stream Analytics](#task-3-configure-stream-analytics)
  - [Exercise 2: Generate and aggregate data](#exercise-2-generate-and-aggregate-data)
    - [Task 1: Run data generator](#task-1-run-data-generator)
    - [Task 2: View aggregate data in Synapse Analytics](#task-2-view-aggregate-data-in-synapse-analytics)
  - [Exercise 3: Cleanup](#exercise-3-cleanup)
    - [Task 1: Stop the data generator](#task-1-stop-the-data-generator)
    - [Task 2: Stop the Stream Analytics job](#task-2-stop-the-stream-analytics-job)
    - [Task 3: Pause the dedicated SQL pool](#task-3-pause-the-dedicated-sql-pool)

## Technology overview

### Azure Stream Analytics

As more and more data is generated from a variety of connected devices and sensors, transforming this data into actionable insights and predictions in near real-time is now an operational necessity. [Azure Stream Analytics](https://docs.microsoft.com/azure/stream-analytics/stream-analytics-introduction) seamlessly integrates with your real-time application architecture to enable powerful, real-time analytics on your data no matter what the volume.

Azure Stream Analytics enables you to develop massively parallel Complex Event Processing (CEP) pipelines with simplicity. It allows you to author powerful, real-time analytics solutions using very simple, declarative [SQL like language](https://docs.microsoft.com/stream-analytics-query/stream-analytics-query-language-reference) with embedded support for temporal logic. Extensive array of [out-of-the-box connectors](https://docs.microsoft.com/azure/stream-analytics/stream-analytics-define-outputs), advanced debugging and job monitoring capabilities help keep costs down by significantly lowering the developer skills required. Additionally, Azure Stream Analytics is highly extensible through support for custom code with [JavaScript User Defined functions](https://docs.microsoft.com/azure/stream-analytics/stream-analytics-javascript-user-defined-functions) further extending the streaming logic written in SQL.

Getting started in seconds is easy with Azure Stream Analytics as there is no infrastructure to worry about, and no servers, virtual machines, or clusters to manage. You can instantly [scale-out the processing power](https://docs.microsoft.com/azure/stream-analytics/stream-analytics-streaming-unit-consumption) from one to hundreds of streaming units for any job. You only pay for the processing used per job.

[Guaranteed event delivery](https://docs.microsoft.com/stream-analytics-query/event-delivery-guarantees-azure-stream-analytics) and an enterprise grade SLA, provide the three 9's of availability, making sure that Azure Stream Analytics is suitable for mission critical workloads. Automated checkpoints enable fault tolerant operation with fast restarts with no data loss.

Azure Stream Analytics can be used to allow you to quickly build real-time dashboards with Power BI for a live command and control view. [Real-time dashboards](https://docs.microsoft.com/azure/stream-analytics/stream-analytics-power-bi-dashboard) help transform live data into actionable and insightful visuals, and help you focus on what matters to you the most.

### Azure Event Hubs

[Azure Event Hubs](https://docs.microsoft.com/azure/event-hubs/event-hubs-about) is a big data pipeline that can ingest millions of events per second. It facilitates the capture, retention, and replay of telemetry and event stream data, using standard protocols such as HTTPS, AMQP, AMQP over websockets, and Kafka. The data can come from many concurrent sources and up to 20 consumer groups can allow applications to read entire event hub independently at their own pace.

## Scenario overview

Contoso Auto is collecting vehicle telemetry and wants to use Event Hubs to rapidly ingest and store the data in its raw form, then do some processing in near real-time. In the end, they want to create a dashboard that automatically updates with new data as it flows in after being processed. What they would like to see on the dashboard are various visualizations of detected anomalies, like engines overheating, abnormal oil pressure, and aggressive driving, using components such as a map to show anomalies related to cities, as well as various charts and graphs depicting this information in a clear way.

In this experience, you will use Azure Event Hubs to ingest streaming vehicle telemetry data as the entry point to a near real-time analytics pipeline built on Event Hubs, Azure Stream Analytics, and Azure Synapse Analytics. Azure Stream Analytics extracts the vehicle sensor data from Event Hubs, performs aggregations over windows of time, then sends the aggregated data to Azure Synapse Analytics for data analysis. A vehicle telemetry data generator will be used to send vehicle telemetry data to Event Hubs.

## Lab setup and pre-requisites

- Azure subscription
- You have successfully completed [Module 0](../00/README.md) to create your lab environment.

## Exercise 0: Start the dedicated SQL pool

This lab uses the dedicated SQL pool. As a first step, make sure it is not paused. If so, start it by following these instructions:

1. Open Synapse Studio (<https://web.azuresynapse.net/>).

2. Select the **Manage** hub.

    ![The manage hub is highlighted.](images/manage-hub.png "Manage hub")

3. Select **SQL pools** in the left-hand menu **(1)**. If the dedicated SQL pool is paused, hover over the name of the pool and select **Resume (2)**.

    ![The resume button is highlighted on the dedicated SQL pool.](images/resume-dedicated-sql-pool.png "Resume")

4. When prompted, select **Resume**. It will take a minute or two to resume the pool.

    ![The resume button is highlighted.](images/resume-dedicated-sql-pool-confirm.png "Resume")

    > **Continue to the next exercise** while the dedicated SQL pool resumes.

## Exercise 1: Configure services

### Task 1: Configure Event Hubs

Azure Event Hubs is a Big Data streaming platform and event ingestion service, capable of receiving and processing millions of events per second. We are using it to temporarily store vehicle telemetry data that is processed and ready to be sent to the real-time dashboard. As data flows into Event Hubs, Azure Stream Analytics will query the data, applying aggregates and tagging anomalies, then send it to Azure Synapse Analytics and Power BI.

In this task, you will create and configure a new event hub within the provided Event Hubs namespace. This will be used to capture vehicle telemetry after it has been processed and enriched by the Azure function you will create later on.

1. Navigate to the [Azure portal](https://portal.azure.com).

2. Select **Resource groups** from the left-hand menu. Then select the resource group named **data-engineering-synapse**.

3. Select the **Event Hubs Namespace** (`eventhubYOUR_UNIQUE_ID`) from the list of resources in your resource group.

    ![The Event Hubs Namespace is selected in the resource group.](images/rg-event-hubs.png "resource group")

4. Within the Event Hubs Namespace blade, select **Event Hubs** within the left-hand menu.

    ![The Event Hubs link is selected in the left-hand menu.](images/event-hubs-link.png 'Event Hubs link')

5. Select the **telemetry** event hub from the list.

    ![The newly created telemetry event hub is selected.](images/event-hubs-select.png 'Event hubs')

6. Select **Shared access policies** from the left-hand menu.

    ![The Shared access policies link is selected in the left-hand menu.](images/event-hubs-shared-access-policies-link.png 'Shared access policies link')

7. Select **+ Add** in the top toolbar to create a new shared access policy.

    ![The Add button is highlighted.](images/event-hubs-shared-access-policies-add-link.png 'Add')

8. In the **Add SAS Policy** blade, configure the following:

    - **Name:** Enter "Read".
    - **Managed:** Unchecked.
    - **Send:** Unchecked.
    - **Listen:** Checked.

        ![The Add SAS Policy form is filled out with the previously mentioned settings entered into the appropriate fields.](images/event-hubs-add-sas-policy-read.png 'Add SAS Policy')

        > It is a best practice to create separate policies for reading, writing, and managing events. This follows the principle of least privilege to prevent services and applications from performing unauthorized operations.

9. Select **Create** on the bottom of the form when you are finished entering the values.

10. Select **+ Add** in the top toolbar to create a new shared access policy.

    ![The Add button is highlighted.](images/event-hubs-shared-access-policies-add-link.png 'Add')

11. In the **Add SAS Policy** blade, configure the following:

    - **Name:** Enter "Write".
    - **Managed:** Unchecked.
    - **Send:** Checked.
    - **Listen:** Unchecked.

        ![The Add SAS Policy form is filled out with the previously mentioned settings entered into the appropriate fields.](images/event-hubs-add-sas-policy-write.png 'Add SAS Policy')

12. Select **Create** on the bottom of the form when you are finished entering the values.

13. Select your **Write** policy from the list. Copy the **Connection string - primary key** value by selecting the Copy button to the right of the field. **SAVE THIS VALUE** in Notepad or similar text editor for later.

    ![The Write policy is selected and its blade displayed. The Copy button next to the Connection string - primary key field is highlighted.](images/event-hubs-write-policy-key.png 'SAS Policy: Write')

### Task 2: Configure Synapse Analytics

Azure Synapse is an end-to-end analytics platform which combines SQL data warehousing, big data analytics, and data integration into a single integrated environment. It empowers users to gain quick access and insights across all of their data, enabling a whole new level of performance and scale that is simply unmatched in the industry.

In this task, you will create a table in a Synapse dedicated SQL pool to store aggregate vehicle data provided by a Stream Analytics job that processes vehicle telemetry ingested by Event Hubs.

1. Navigate to the [Azure portal](https://portal.azure.com).

2. Select **Resource groups** from the left-hand menu. Then select the resource group named **data-engineering-synapse**.

3. Select the **Synapse workspace** (`asaworkspaceYOUR_UNIQUE_ID`) from the list of resources in your resource group.

    ![The Synapse workspace is selected in the resource group.](images/rg-synapse-workspace.png "resource group")

4. Select **Open** within the **Open Synapse Studio** box inside the Overview pane.

    ![The Open link is highlighted.](images/open-synapse-studio.png "Open Synapse Studio")

5. Within Synapse Studio, select **Data** in the left-hand menu to navigate to the Data hub.

    ![The Data hub is highlighted.](images/data-hub.png "Data hub")

6. Select the **Workspace** tab **(1)**, expand Databases and right-click **SQLPool01 (2)**. Select **New SQL script (3)**, then select **Empty script (4)**.

    ![The New SQL script option is highlighted in the SQLPool01 context menu.](images/synapse-new-script.png "New SQL script")

7. Make sure the script is connected to `SQLPool01`, then replace the script with the following and select **Run** to create a new table:

    ```sql
    CREATE TABLE dbo.VehicleAverages
    ( 
        [AverageEngineTemperature] [float] NOT  NULL,
        [AverageSpeed] [float] NOT  NULL,
        [Snapshot] [datetime] NOT  NULL
    )
    WITH
    (
        DISTRIBUTION = ROUND_ROBIN,
        CLUSTERED COLUMNSTORE INDEX
    )
    GO
    ```

    ![The script is displayed.](images/synapse-new-table-script.png "New table script")

### Task 3: Configure Stream Analytics

Azure Stream Analytics is an event-processing engine that allows you to examine high volumes of data streaming from devices. Incoming data can be from devices, sensors, web sites, social media feeds, applications, and more. It also supports extracting information from data streams, identifying patterns, and relationships. You can then use these patterns to trigger other actions downstream, such as create alerts, feed information to a reporting tool, or store it for later use.

In this task, you will configure Stream Analytics to use the event hub you created as a source, query and analyze that data, then send it to Power BI for reporting and aggregated data to Azure Synapse Analytics.

1. Navigate to the [Azure portal](https://portal.azure.com).

2. Select **Resource groups** from the left-hand menu. Then select the resource group named **data-engineering-synapse**.

3. Select the **Stream Analytics job** (`asaYOUR_UNIQUE_ID`) from the list of resources in your resource group.

    ![The Stream Analytics job is selected in the resource group.](images/rg-stream-analytics.png "resource group")

4. Within the Stream Analytics job, select **Storage account settings** in the left-hand menu, then select **Add storage account**. Since we will use Synapse Analytics as one of the outputs, we need to first configure the job storage account.

    ![The storage account settings link and add storage account button are highlighted.](images/asa-storage-account.png "Storage account settings")

5. In the **Storage account settings** form, configure the following:

   - **Select storage account from your subscriptions:** Selected.
   - **Subscription:** Make sure the subscription you are using for this lab is selected.
   - **Storage account:** Select the storage account named **asadatalakeYOUR_UNIQUE_ID**.
   - **Authentication mode:** Select "Connection string".

        ![The form is configured as described.](images/asa-storage-account-form.png "Storage account settings")

6. Select **Save**, then **Yes** when prompted to save the storage account settings.

7. Within the Stream Analytics job, select **Inputs** within the left-hand menu.

    ![The Inputs link is selected in the left-hand menu.](images/inputs-link.png 'Inputs link')

8. Select **+ Add stream input** in the top toolbar, then select **Event Hub** to create a new Event Hub input.

    ![The Add stream input button and Event Hub menu item are highlighted.](images/stream-analytics-add-input-link.png 'Add stream input - Event Hub')

9. In the **New Input** blade, configure the following:

    - **Name:** Enter "eventhub".
    - **Select Event Hub from your subscriptions:** Selected.
    - **Subscription:** Make sure the subscription you are using for this lab is selected.
    - **Event Hub namespace:** Select the Event Hub namespace you are using for this lab.
    - **Event Hub name:** Select **Use existing**, then select **telemetry**, which you created earlier.
    - **Event Hub consumer group:** Select **Use existing**, then select **$Default**.
    - **Authentication mode:** Select **Connection string**.
    - **Event Hub policy name:** Select **Use existing**, then select **Read**.
    - Leave all other values at their defaults.

        ![The New Input form is filled out with the previously mentioned settings entered into the appropriate fields.](images/stream-analytics-new-input.png 'New Input')

10. Select **Save** on the bottom of the form when you are finished entering the values.

11. Within the Stream Analytics job blade, select **Outputs** within the left-hand menu.

    ![The Outputs link is selected in the left-hand menu.](images/outputs-link.png 'Outputs link')

12. Select **+ Add** in the top toolbar, then select **Azure Synapse Analytics** to create a new Synapse Analytics output.

    ![The Azure Synapse Analytics menu item is highlighted.](images/stream-analytics-add-output-synapse-link.png "Add output - Azure Synapse Analytics")

13. In the **New Output** blade, configure the following:

    - **Output alias:** Enter "synapse".
    - **Select Azure Synapse Analytics from your subscriptions:** Selected.
    - **Subscription:** Select the subscription you are using for this lab.
    - **Database:** Select "SQLPool01". Make sure your correct Synapse workspace name appears under "Server name".
    - **Table:** Enter `dbo.VehicleAverages`
    - **Authentication mode:** Select "Connection string".
    - **Username:** Enter `asa.sql.admin` (or the dedicated SQL pool username provided to you for this lab)
    - **Password:** Enter the SQL admin password value you entered when deploying the lab environment, or which was provided to you as part of your hosted lab environment. **Note**: This password is most likely *not* the same as the password you used to sign in to the Azure portal.

        ![The New Output form is filled out with the previously mentioned settings entered into the appropriate fields.](images/synapse-new-output.png "New Output")

        > **Note:** If you are unsure about your SQL admin username, navigate to the Synapse workspace in the Azure resource group. The SQL admin username is shown in the Overview pane.

        ![The SQL admin account name is displayed.](images/sql-admin-name.png "SQL admin name")

14. Select **Save** on the bottom of the form when you are finished entering the values.

15. Within the Stream Analytics job blade, select **Query** within the left-hand menu.

    ![The Query link is selected in the left-hand menu.](images/query-link.png 'Query link')

16. Clear the edit **Query** window and paste the following in its place:

    ```sql
    WITH
    VehicleAverages AS (
        select
            AVG(engineTemperature) averageEngineTemperature,
            AVG(speed) averageSpeed,
            System.TimeStamp() as snapshot
        FROM
            eventhub TIMESTAMP BY [timestamp]
        GROUP BY
            TumblingWindow(Duration(minute, 2))
    )
    -- INSERT INTO SYNAPSE ANALYTICS
    SELECT
        *
    INTO
        synapse
    FROM
        VehicleAverages
    ```

    ![The query above has been inserted into the Query window.](images/stream-analytics-query.png 'Query window')

    The query averages the engine temperature and speed over a two second duration. The query aggregates the average engine temperature and speed of all vehicles over the past two minutes, using `TumblingWindow(Duration(minute, 2))`, and outputs these fields to the `synapse` output.

17. Select **Save query** in the top toolbar when you are finished updating the query.

18. Within the Stream Analytics job blade, select **Overview** within the left-hand menu. On top of the Overview blade, select **Start**.

    ![The Start button is highlighted on top of the Overview blade.](images/stream-analytics-overview-start-button.png 'Overview')

19. In the Start job blade that appears, select **Now** for the job output start time, then select **Start**. This will start the Stream Analytics job so it will be ready to start processing and sending your events to Power BI later on.

    ![The Now and Start buttons are highlighted within the Start job blade.](images/stream-analytics-start-job.png 'Start job')

## Exercise 2: Generate and aggregate data

### Task 1: Run data generator

The data generator console application creates and sends simulated vehicle sensor telemetry for an array of vehicles (denoted by VIN (vehicle identification number)) directly to Event Hubs. For this to happen, you first need to configure it with the Event Hub connection string.

In this task, you will configure and run the data generator. The data generator saves simulated vehicle telemetry data to Event Hubs, prompting your Stream Analytics job to aggregate and analyze the enriched data and send it to Power BI and Synapse Analytics. The final step will be to create the Power BI report in the task that follows.

1. On your lab VM or computer, download the [TransactionGeneratorExecutable.zip](https://solliancepublicdata.blob.core.windows.net/dataengineering/dp-203/TransactionGeneratorExecutable.zip) file.

2. Extract the zip file to your machine, making note of the extraction location.

3. Open the folder containing the extracted files, then open either the `linux-x64`, `osx-x64`, or `win-x64` subfolder, based on your environment.

4. Within the appropriate subfolder, open the **appsettings.json** file. Paste your `telemetry` Event Hub connection string value next to `EVENT_HUB_CONNECTION_STRING`. Make sure you have quotes ("") around the value, as shown. **Save** the file.

    ![The Event Hub connection string is highlighted within the appsettings.json file.](images/appsettings.png "appsettings.json")

    > **Note:** Make sure that the connection string ends with `EntityPath=telemetry` (eg. `Endpoint=sb://YOUR_EVENTHUB_NAMESPACE.servicebus.windows.net/;SharedAccessKeyName=Write;SharedAccessKey=REDACTED/S/U=;EntityPath=telemetry`). If not, then you did not copy the connection string from the `Write` policy of your event hub.

    `SECONDS_TO_LEAD` is the amount of time to wait before sending vehicle telemetry data. Default value is `0`.

    `SECONDS_TO_RUN` is the maximum amount of time to allow the generator to run before stopping transmission of data. The default value is `1800`. Data will also stop transmitting when you enter Ctrl+C while the generator is running, or if you close the window.

5. Execute the data generator using one of the following methods, based on your platform:

   1. Windows:

      * Simply execute **TransactionGenerator.exe** inside the `win-x64` folder.

   2. Linux:

      * Navigate to the `linux-x64` folder.
      * Run `chmod 777 DataGenerator` to provide access to the binary.
      * Run `./DataGenerator`.

   3. MacOS:

      * Open a new terminal.
      * Navigate to the `osx-x64` directory.
      * Run `./DataGenerator`.

6. If you are using Windows and receive a dialog after trying to execute the data generator, select **More info**, then **Run anyway**.

    ![More info is highlighted.](images/microsoft-defender-moreinfo.png "Windows protected your PC")

    ![The Run anyway button is highlighted.](images/microsoft-defender-runanyway.png "Run anyway")

6.  A new console window will open, and you should see it start to send data after a few seconds. Once you see that it is sending data to Event Hubs, _minimize_ the window and keep it running in the background. Allow this to run for a mimimum of three minutes before moving onto the next step.

    ![Screenshot of the console window.](images/transaction-generator.png 'Console window')

    After every 500 records are requested to be sent, you will see output statistics.

### Task 2: View aggregate data in Synapse Analytics

As you recall, when you created the query in Stream Analytics, you aggregated the engine temperature and vehicle speed data over two-minute intervals and saved the data to Synapse Analytics. This capability demonstrates the Stream Analytics query's ability to write data to multiple outputs at varying intervals. Writing to a Synapse Analytics dedicated SQL pool enables us to retain the historic and current aggregate data as part of the data warehouse without requiring an ETL/ELT process.

In this task, you will view the anomaly data within Synapse Analytics.

1. If you have not yet done so, **stop** the **TransactionGenerator**.

2. Navigate to the [Azure portal](https://portal.azure.com).

3. Select **Resource groups** from the left-hand menu. Then select the resource group named **data-engineering-synapse**.

4. Select the **Synapse workspace** (`asaworkspaceYOUR_UNIQUE_ID`) from the list of resources in your resource group.

    ![The Synapse workspace is selected in the resource group.](images/rg-synapse-workspace.png "resource group")

5. Select **Open** within the **Open Synapse Studio** box inside the Overview pane.

    ![The Open link is highlighted.](images/open-synapse-studio.png "Open Synapse Studio")

6. Within Synapse Studio, select **Data** in the left-hand menu to navigate to the Data hub.

    ![The Data hub is highlighted.](images/data-hub.png "Data hub")

7. Select the **Workspace** tab **(1)**, expand the `SQLPool01` database, expand `Tables`, then right-click on the **dbo.VehicleAverages** table **(2)**. If you do not see the table listed, refresh the tables list. Select **New SQL script (3)**, then **Select TOP 100 rows (4)**.

    ![The Select TOP 100 rows menu item is selected.](images/select-top-100-rows.png "Select TOP 100 rows")

8. View the query results and observe the aggregate data stored in `AverageEngineTemperature` and `AverageSpeed`. The `Snapshot` value changes in two-minute intervals between these records.

   ![The VehicleAverages table output is displayed.](images/synapse-vehicleaverage-table.png "VehicleAverages results")

9. Select the **Chart** view in the Results output, then set the chart type to **Area**. This visualization shows the average engine temperature correlated with the average speed over time. Feel free to experiment with the chart settings. The longer the data generator runs the more data points are generated. The following visualization is for an example of a  session that ran over 10 mins, and may not represent what you see on the screen.

![The chart view is displayed.](images/synapse-vehicleaverage-chart.png "VehicleAverages chart")

## Exercise 3: Cleanup

Complete these steps to stop the data generator and free up resources you no longer need.

### Task 1: Stop the data generator

1. Go back to the console/terminal window in which your data generator is running. Close the window to stop the generator.

### Task 2: Stop the Stream Analytics job

1. Navigate to the Stream Analytics job in the Azure portal.

2. In the Overview pane, select **Stop**, then select **Yes** when prompted.

    ![The stop button is highlighted.](images/asa-stop.png "Stop")

### Task 3: Pause the dedicated SQL pool

1. Open Synapse Studio (<https://web.azuresynapse.net/>).

2. Select the **Manage** hub.

    ![The manage hub is highlighted.](images/manage-hub.png "Manage hub")

3. Select **SQL pools** in the left-hand menu **(1)**. Hover over the name of the dedicated SQL pool and select **Pause (2)**.

    ![The pause button is highlighted on the dedicated SQL pool.](images/pause-dedicated-sql-pool.png "Pause")

4. When prompted, select **Pause**.

    ![The pause button is highlighted.](images/pause-dedicated-sql-pool-confirm.png "Pause")
