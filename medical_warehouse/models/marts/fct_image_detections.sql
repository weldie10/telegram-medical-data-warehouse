{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- Fact table for image detections
-- Joins YOLO detection results with message facts to enable analysis
-- of image content patterns and their relationship to engagement metrics

with yolo_detections as (
    select * from {{ source('raw', 'yolo_detections') }}
),

fct_messages as (
    select * from {{ ref('fct_messages') }}
),

dim_channels as (
    select * from {{ ref('dim_channels') }}
)

select
    -- Fact key (combination of message_id and channel_key for uniqueness)
    yd.message_id,
    fm.channel_key,
    fm.date_key,
    
    -- Detection information
    yd.image_category,
    coalesce(yd.num_detections, 0)::integer as num_detections,
    coalesce(yd.max_confidence, 0.0)::decimal(5,4) as max_confidence,
    yd.detected_classes,
    
    -- Individual detection details (first 5 detections)
    yd.detected_class_1,
    yd.confidence_1,
    yd.detected_class_2,
    yd.confidence_2,
    yd.detected_class_3,
    yd.confidence_3,
    yd.detected_class_4,
    yd.confidence_4,
    yd.detected_class_5,
    yd.confidence_5,
    
    -- Message engagement metrics (for analysis)
    fm.view_count,
    fm.forward_count,
    
    -- Metadata
    yd.image_path,
    yd.loaded_at as detection_loaded_at

from yolo_detections yd
inner join dim_channels dc
    on yd.channel_name = dc.channel_name
inner join fct_messages fm
    on yd.message_id = fm.message_id
    and dc.channel_key = fm.channel_key
