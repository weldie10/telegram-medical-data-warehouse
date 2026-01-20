-- Analysis Queries for Task 3: Data Enrichment with Object Detection
-- These queries help answer the analysis questions in the report

-- ============================================================================
-- Question 1: Do "promotional" posts (with people) get more views than 
--             "product_display" posts?
-- ============================================================================

-- Average views by image category
SELECT 
    image_category,
    COUNT(*) as post_count,
    AVG(view_count) as avg_views,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY view_count) as median_views,
    MIN(view_count) as min_views,
    MAX(view_count) as max_views,
    SUM(view_count) as total_views
FROM marts.fct_image_detections
GROUP BY image_category
ORDER BY avg_views DESC;

-- Statistical comparison: Promotional vs Product Display
WITH category_stats AS (
    SELECT 
        image_category,
        AVG(view_count) as avg_views,
        STDDEV(view_count) as stddev_views,
        COUNT(*) as n
    FROM marts.fct_image_detections
    WHERE image_category IN ('promotional', 'product_display')
    GROUP BY image_category
)
SELECT 
    'promotional' as category,
    avg_views,
    stddev_views,
    n
FROM category_stats
WHERE image_category = 'promotional'
UNION ALL
SELECT 
    'product_display' as category,
    avg_views,
    stddev_views,
    n
FROM category_stats
WHERE image_category = 'product_display';

-- ============================================================================
-- Question 2: Which channels use more visual content?
-- ============================================================================

-- Image category distribution by channel
SELECT 
    dc.channel_name,
    dc.channel_type,
    COUNT(DISTINCT fid.message_id) as images_with_detections,
    COUNT(DISTINCT CASE WHEN fid.image_category = 'promotional' THEN fid.message_id END) as promotional_count,
    COUNT(DISTINCT CASE WHEN fid.image_category = 'product_display' THEN fid.message_id END) as product_display_count,
    COUNT(DISTINCT CASE WHEN fid.image_category = 'lifestyle' THEN fid.message_id END) as lifestyle_count,
    COUNT(DISTINCT CASE WHEN fid.image_category = 'other' THEN fid.message_id END) as other_count,
    ROUND(
        COUNT(DISTINCT CASE WHEN fid.image_category = 'promotional' THEN fid.message_id END)::numeric / 
        NULLIF(COUNT(DISTINCT fid.message_id), 0) * 100, 
        2
    ) as promotional_percentage,
    ROUND(
        COUNT(DISTINCT CASE WHEN fid.image_category = 'product_display' THEN fid.message_id END)::numeric / 
        NULLIF(COUNT(DISTINCT fid.message_id), 0) * 100, 
        2
    ) as product_display_percentage
FROM marts.fct_image_detections fid
INNER JOIN marts.dim_channels dc
    ON fid.channel_key = dc.channel_key
GROUP BY dc.channel_name, dc.channel_type
ORDER BY images_with_detections DESC;

-- Total images per channel (including those without detections)
SELECT 
    dc.channel_name,
    dc.channel_type,
    COUNT(DISTINCT fm.message_id) as total_messages_with_images,
    COUNT(DISTINCT fid.message_id) as messages_with_detections,
    ROUND(
        COUNT(DISTINCT fid.message_id)::numeric / 
        NULLIF(COUNT(DISTINCT fm.message_id), 0) * 100, 
        2
    ) as detection_coverage_percentage
FROM marts.fct_messages fm
LEFT JOIN marts.fct_image_detections fid
    ON fm.message_id = fid.message_id 
    AND fm.channel_key = fid.channel_key
INNER JOIN marts.dim_channels dc
    ON fm.channel_key = dc.channel_key
WHERE fm.has_image = true
GROUP BY dc.channel_name, dc.channel_type
ORDER BY total_messages_with_images DESC;

-- ============================================================================
-- Additional Analysis: Detection Quality and Patterns
-- ============================================================================

-- Detection confidence distribution
SELECT 
    image_category,
    CASE 
        WHEN max_confidence >= 0.8 THEN 'High (>=0.8)'
        WHEN max_confidence >= 0.5 THEN 'Medium (0.5-0.8)'
        ELSE 'Low (<0.5)'
    END as confidence_level,
    COUNT(*) as count,
    AVG(num_detections) as avg_detections_per_image
FROM marts.fct_image_detections
GROUP BY image_category, confidence_level
ORDER BY image_category, confidence_level;

-- Most common detected objects
SELECT 
    detected_class_1 as detected_object,
    COUNT(*) as detection_count,
    AVG(confidence_1) as avg_confidence
FROM marts.fct_image_detections
WHERE detected_class_1 IS NOT NULL
GROUP BY detected_class_1
ORDER BY detection_count DESC
LIMIT 20;

-- Engagement metrics by image category and channel
SELECT 
    dc.channel_name,
    fid.image_category,
    COUNT(*) as post_count,
    AVG(fid.view_count) as avg_views,
    AVG(fid.forward_count) as avg_forwards,
    AVG(fid.num_detections) as avg_detections,
    AVG(fid.max_confidence) as avg_confidence
FROM marts.fct_image_detections fid
INNER JOIN marts.dim_channels dc
    ON fid.channel_key = dc.channel_key
GROUP BY dc.channel_name, fid.image_category
ORDER BY dc.channel_name, avg_views DESC;
