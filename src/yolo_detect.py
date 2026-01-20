"""
YOLO Object Detection Script for Medical Data Warehouse

This script uses YOLOv8 to detect objects in images downloaded from Telegram channels
and categorizes them based on detected objects (person, bottle, container, etc.).
"""

import csv
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import colorlog
from ultralytics import YOLO

# Add parent directory to path for imports
BASE_DIR = Path(__file__).parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# Configuration
IMAGES_DIR = BASE_DIR / "data" / "raw" / "images"
OUTPUT_CSV = BASE_DIR / "data" / "raw" / "yolo_detections.csv"
MODEL_NAME = "yolov8n.pt"  # YOLOv8 nano model for efficiency

# YOLO class IDs (COCO dataset)
CLASS_PERSON = 0
CLASS_BOTTLE = 39
CLASS_CUP = 41
CLASS_BOWL = 45
CLASS_BANANA = 46  # Sometimes detects similar shapes
CLASS_APPLE = 47

# Product-related classes (containers, bottles, etc.)
PRODUCT_CLASSES = {CLASS_BOTTLE, CLASS_CUP, CLASS_BOWL}

# Confidence threshold
CONFIDENCE_THRESHOLD = 0.25


def setup_logging() -> colorlog.Logger:
    """Configure colored logging."""
    logger = colorlog.getLogger("yolo_detector")
    logger.setLevel(colorlog.INFO)
    
    console_handler = colorlog.StreamHandler(sys.stdout)
    console_handler.setLevel(colorlog.INFO)
    console_format = colorlog.ColoredFormatter(
        "%(log_color)s%(levelname)s%(reset)s - %(message)s",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "red,bg_white",
        },
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    return logger


def extract_message_id_from_path(image_path: Path) -> Optional[int]:
    """
    Extract message_id from image filename.
    
    Images are stored as: data/raw/images/{channel_name}/{message_id}.jpg
    
    Args:
        image_path: Path to the image file
        
    Returns:
        Message ID as integer, or None if extraction fails
    """
    try:
        # Get filename without extension
        filename = image_path.stem
        return int(filename)
    except (ValueError, AttributeError):
        return None


def extract_channel_name_from_path(image_path: Path) -> Optional[str]:
    """
    Extract channel name from image path.
    
    Args:
        image_path: Path to the image file
        
    Returns:
        Channel name or None if extraction fails
    """
    try:
        # Path structure: data/raw/images/{channel_name}/{message_id}.jpg
        # Get parent directory name
        return image_path.parent.name
    except (AttributeError, IndexError):
        return None


def classify_image(detections: List[Dict]) -> str:
    """
    Classify image based on detected objects.
    
    Classification scheme:
    - promotional: Contains person + product (bottle/container)
    - product_display: Contains bottle/container, no person
    - lifestyle: Contains person, no product
    - other: Neither detected
    
    Args:
        detections: List of detection dictionaries with 'class' and 'confidence'
        
    Returns:
        Image category string
    """
    has_person = False
    has_product = False
    
    for detection in detections:
        class_id = detection.get('class')
        confidence = detection.get('confidence', 0)
        
        if confidence < CONFIDENCE_THRESHOLD:
            continue
            
        if class_id == CLASS_PERSON:
            has_person = True
        elif class_id in PRODUCT_CLASSES:
            has_product = True
    
    # Classification logic
    if has_person and has_product:
        return "promotional"
    elif has_product and not has_person:
        return "product_display"
    elif has_person and not has_product:
        return "lifestyle"
    else:
        return "other"


def detect_objects_in_image(
    model: YOLO,
    image_path: Path,
    logger: colorlog.Logger,
) -> Tuple[List[Dict], str]:
    """
    Run YOLO detection on a single image.
    
    Args:
        model: YOLO model instance
        image_path: Path to the image file
        logger: Logger instance
        
    Returns:
        Tuple of (detections list, image_category)
    """
    try:
        # Run inference
        results = model(str(image_path), conf=CONFIDENCE_THRESHOLD, verbose=False)
        
        detections = []
        if results and len(results) > 0:
            result = results[0]
            
            # Extract detections
            if result.boxes is not None:
                boxes = result.boxes
                for i in range(len(boxes)):
                    class_id = int(boxes.cls[i].item())
                    confidence = float(boxes.conf[i].item())
                    class_name = model.names[class_id]
                    
                    detections.append({
                        'class': class_id,
                        'class_name': class_name,
                        'confidence': confidence,
                    })
        
        # Classify image
        image_category = classify_image(detections)
        
        return detections, image_category
    
    except Exception as e:
        logger.error(f"Error processing image {image_path}: {str(e)}")
        return [], "other"


def find_all_images(images_dir: Path) -> List[Path]:
    """
    Find all image files in the images directory.
    
    Args:
        images_dir: Base directory containing channel subdirectories
        
    Returns:
        List of image file paths
    """
    image_files = []
    
    if not images_dir.exists():
        return image_files
    
    # Supported image extensions
    image_extensions = {'.jpg', '.jpeg', '.png', '.bmp', '.gif'}
    
    # Recursively find all image files
    for ext in image_extensions:
        image_files.extend(images_dir.rglob(f"*{ext}"))
        image_files.extend(images_dir.rglob(f"*{ext.upper()}"))
    
    return sorted(image_files)


def process_images(
    model: YOLO,
    images: List[Path],
    logger: colorlog.Logger,
) -> List[Dict]:
    """
    Process all images and collect detection results.
    
    Args:
        model: YOLO model instance
        images: List of image file paths
        logger: Logger instance
        
    Returns:
        List of detection result dictionaries
    """
    results = []
    total = len(images)
    
    logger.info(f"Processing {total} images...")
    
    for idx, image_path in enumerate(images, 1):
        # Extract metadata
        message_id = extract_message_id_from_path(image_path)
        channel_name = extract_channel_name_from_path(image_path)
        
        if not message_id:
            logger.warning(f"Could not extract message_id from {image_path}, skipping")
            continue
        
        if not channel_name:
            logger.warning(f"Could not extract channel_name from {image_path}, skipping")
            continue
        
        # Run detection
        detections, image_category = detect_objects_in_image(model, image_path, logger)
        
        # Get highest confidence detection for summary
        max_confidence = max([d['confidence'] for d in detections], default=0.0)
        detected_classes = [d['class_name'] for d in detections]
        
        result = {
            'message_id': message_id,
            'channel_name': channel_name,
            'image_path': str(image_path.relative_to(BASE_DIR)),
            'image_category': image_category,
            'num_detections': len(detections),
            'max_confidence': max_confidence,
            'detected_classes': ', '.join(detected_classes) if detected_classes else '',
        }
        
        # Add individual detections
        for i, detection in enumerate(detections):
            result[f'detected_class_{i+1}'] = detection['class_name']
            result[f'confidence_{i+1}'] = detection['confidence']
        
        results.append(result)
        
        if idx % 50 == 0:
            logger.info(f"Processed {idx}/{total} images...")
    
    return results


def save_results_to_csv(results: List[Dict], output_path: Path, logger: colorlog.Logger):
    """
    Save detection results to CSV file.
    
    Args:
        results: List of detection result dictionaries
        output_path: Path to output CSV file
        logger: Logger instance
    """
    if not results:
        logger.warning("No results to save")
        return
    
    # Get all possible column names
    all_columns = set()
    for result in results:
        all_columns.update(result.keys())
    
    # Define column order (important columns first)
    priority_columns = [
        'message_id',
        'channel_name',
        'image_path',
        'image_category',
        'num_detections',
        'max_confidence',
        'detected_classes',
    ]
    
    # Add detection columns (detected_class_1, confidence_1, etc.)
    detection_columns = []
    for i in range(1, 21):  # Support up to 20 detections per image
        detection_columns.extend([f'detected_class_{i}', f'confidence_{i}'])
    
    # Combine columns
    columns = priority_columns + [col for col in detection_columns if col in all_columns]
    
    # Write CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(results)
    
    logger.info(f"Saved {len(results)} detection results to {output_path}")


def main():
    """Main function to orchestrate YOLO detection."""
    logger = setup_logging()
    
    logger.info("=" * 60)
    logger.info("YOLO Object Detection for Medical Data Warehouse")
    logger.info("=" * 60)
    
    # Check images directory
    if not IMAGES_DIR.exists():
        logger.error(f"Images directory does not exist: {IMAGES_DIR}")
        sys.exit(1)
    
    # Find all images
    images = find_all_images(IMAGES_DIR)
    
    if not images:
        logger.warning(f"No images found in {IMAGES_DIR}")
        sys.exit(0)
    
    logger.info(f"Found {len(images)} images to process")
    logger.info(f"Using model: {MODEL_NAME}")
    
    # Load YOLO model
    try:
        logger.info("Loading YOLO model...")
        model = YOLO(MODEL_NAME)
        logger.info("Model loaded successfully")
    except Exception as e:
        logger.error(f"Failed to load YOLO model: {str(e)}")
        sys.exit(1)
    
    # Process images
    results = process_images(model, images, logger)
    
    # Save results
    save_results_to_csv(results, OUTPUT_CSV, logger)
    
    # Summary statistics
    logger.info("=" * 60)
    logger.info("Detection Summary")
    logger.info("=" * 60)
    
    if results:
        categories = {}
        for result in results:
            category = result['image_category']
            categories[category] = categories.get(category, 0) + 1
        
        logger.info("Image categories:")
        for category, count in sorted(categories.items()):
            logger.info(f"  {category}: {count}")
        
        total_detections = sum(r['num_detections'] for r in results)
        avg_detections = total_detections / len(results) if results else 0
        logger.info(f"\nTotal detections: {total_detections}")
        logger.info(f"Average detections per image: {avg_detections:.2f}")
    
    logger.info("=" * 60)
    logger.info("Detection complete!")


if __name__ == "__main__":
    main()
