{
  "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#",
  "contentVersion": "1.0.0.0",
  "name": "FleetOperationsBatchPipeline",
  "description": "Azure Data Factory pipeline — triggers Databricks fleet analytics job daily at 03:00 SAST",
  "properties": {
    "activities": [
      {
        "name": "IngestAndComputeKPIs",
        "type": "DatabricksNotebook",
        "dependsOn": [],
        "typeProperties": {
          "notebookPath": "/Repos/fleet-operations-analytics/scripts/run_pipeline",
          "baseParameters": {
            "run_date": {
              "value": "@formatDateTime(utcnow(), 'yyyy-MM-dd')",
              "type": "Expression"
            },
            "observation_days": "30",
            "env": "production"
          }
        },
        "linkedServiceName": {
          "referenceName": "AzureDatabricksLinkedService",
          "type": "LinkedServiceReference"
        }
      },
      {
        "name": "ValidateOutputs",
        "type": "DatabricksNotebook",
        "dependsOn": [
          {
            "activity": "IngestAndComputeKPIs",
            "dependencyConditions": ["Succeeded"]
          }
        ],
        "typeProperties": {
          "notebookPath": "/Repos/fleet-operations-analytics/scripts/validate_outputs",
          "baseParameters": {
            "run_date": {
              "value": "@formatDateTime(utcnow(), 'yyyy-MM-dd')",
              "type": "Expression"
            }
          }
        },
        "linkedServiceName": {
          "referenceName": "AzureDatabricksLinkedService",
          "type": "LinkedServiceReference"
        }
      },
      {
        "name": "NotifyOnFailure",
        "type": "WebActivity",
        "dependsOn": [
          {
            "activity": "IngestAndComputeKPIs",
            "dependencyConditions": ["Failed"]
          }
        ],
        "typeProperties": {
          "url": "@pipeline().parameters.alert_webhook_url",
          "method": "POST",
          "body": {
            "text": "Fleet pipeline FAILED on @{formatDateTime(utcnow(), 'yyyy-MM-dd')}. Check ADF run ID: @{pipeline().RunId}"
          }
        }
      }
    ],
    "triggers": [
      {
        "name": "DailyTrigger_0300_SAST",
        "type": "ScheduleTrigger",
        "typeProperties": {
          "recurrence": {
            "frequency": "Day",
            "interval": 1,
            "startTime": "2024-01-01T01:00:00Z",
            "timeZone": "UTC"
          }
        }
      }
    ],
    "parameters": {
      "alert_webhook_url": {
        "type": "String"
      }
    }
  }
}
