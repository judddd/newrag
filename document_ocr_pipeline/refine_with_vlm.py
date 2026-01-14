#!/usr/bin/env python3
"""
使用VLM模型优化OCR结果，生成适合ES检索的规范化JSON
支持LM Studio本地部署的模型
"""
import os
import sys
import json
import base64
import argparse
from pathlib import Path
from typing import Dict, Any, List
from openai import OpenAI


class VLMRefiner:
    """使用VLM模型优化OCR结果"""
    
    def __init__(self, api_base: str = "http://localhost:1234/v1", api_key: str = "lm-studio"):
        """
        初始化VLM精炼器
        
        Args:
            api_base: LM Studio API地址
            api_key: API密钥（LM Studio默认不需要）
        """
        self.client = OpenAI(base_url=api_base, api_key=api_key)
        print(f"✓ Connected to LM Studio at {api_base}")
    
    def encode_image_base64(self, image_path: str) -> str:
        """将图片编码为base64"""
        with open(image_path, 'rb') as f:
            return base64.b64encode(f.read()).decode('utf-8')
    
    def generate_cleaned_text(self, image_path: str, ocr_text: str, model: str = None) -> str:
        """
        第一次 VLM 调用：生成清理后的文本内容
        
        Args:
            image_path: 图片路径
            ocr_text: 原始 OCR 文本
            model: 模型名称
            
        Returns:
            清理后的纯文本字符串
        """
        print("🤖 [1/2] Calling VLM to clean OCR text...")
        
        image_base64 = self.encode_image_base64(image_path)
        
        prompt = f"""You are a text correction expert. Your task is to fix OCR errors and clean up the text.

**Original OCR Text:**
{ocr_text}

**Your Task:**
Fix OCR errors, correct garbled text, and clean up the content based on what you see in the image.

Common OCR errors to fix:
- Date formats: "4-AU9-25" → "4-Aug-25", "伛 SeP 3" → "15-Sep-25"
- Letter/number confusion: "l0" → "10", "O" → "0"
- Garbled characters: Remove or correct based on image context
- Spacing issues: Fix abnormal spaces

**CRITICAL:** 
- Output ONLY the corrected text
- NO explanations, NO JSON, NO formatting
- Just the clean, corrected text content

Output the corrected text:"""

        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_base64}"}},
                    {"type": "text", "text": prompt}
                ]
            }
        ]
        
        # 重试逻辑：最多 3 次
        max_retries = 3
        for retry in range(max_retries):
            try:
                if retry > 0:
                    print(f"   🔄 Retry {retry}/{max_retries}...")
                
                response = self.client.chat.completions.create(
                    model=model if model else "local-model",
                    messages=messages,
                    max_tokens=4096,
                    temperature=0.1
                )
                
                cleaned_text = response.choices[0].message.content.strip()
                
                if not cleaned_text:
                    raise ValueError("VLM returned empty text")
                
                print(f"   ✓ Cleaned text generated ({len(cleaned_text)} chars)")
                return cleaned_text
                
            except Exception as e:
                if retry < max_retries - 1:
                    print(f"   ⚠️  Error: {e} - will retry")
                    continue
                else:
                    print(f"   ❌ All retries failed: {e}")
                    print(f"   → Fallback: using original OCR text")
                    return ocr_text
        
        # 不应该到达这里
        return ocr_text
    
    def generate_visual_description(self, image_path: str, cleaned_text: str, model: str = None) -> str:
        """
        第二次 VLM 调用：生成视觉描述
        
        Args:
            image_path: 图片路径
            cleaned_text: 第一次调用生成的清理后文本
            model: 模型名称
            
        Returns:
            视觉描述的纯文本字符串（200-400字）
        """
        print("🤖 [2/2] Calling VLM to generate visual description...")
        
        image_base64 = self.encode_image_base64(image_path)
        
        prompt = f"""You are a technical document analyzer. Describe EVERYTHING you see in this document page.

**Reference Text Content (for context):**
{cleaned_text[:500]}...

**Your Task:**
Write a comprehensive 200-400 word description of what you SEE in the image.

**Must Include:**
1. **Layout & Structure:**
   - Page orientation, columns, sections, organization
   - Visual element positions and counts

2. **Visual Elements:**
   - Tables, diagrams, charts, photos, stamps, logos
   - Count them: "2 tables", "1 circuit diagram", etc.

3. **Technical Details (CRITICAL):**
   - ALL part numbers: R1, C5, U2, IC3, J1, etc.
   - Model codes and device IDs
   - Specifications and measurements
   - Reference designators and catalog numbers

4. **Colors & Styling:**
   - Dominant colors, highlights, color-coded elements
   - Text hierarchy and styling

5. **Text Content:**
   - Visible titles, headings, labels, annotations
   - Key identifiers and codes

6. **Unique Features:**
   - Watermarks, stamps (describe color and position)
   - Handwritten notes, signatures
   - Quality issues or damage

**CRITICAL Requirements:**
- Write 200-400 words as a SINGLE continuous paragraph
- Include EVERY visible part number, model code, technical identifier
- Be specific about positions, colors, and counts
- This will be used for technical search queries like:
  * "Find circuit diagram with IC U2"
  * "Search for pages with red approval stamp"
  * "Locate specifications for connector J1"

**Output Format:**
- ONLY the description text
- NO JSON, NO formatting, NO sections
- Just one continuous paragraph (200-400 words)

Write the description:"""

        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_base64}"}},
                    {"type": "text", "text": prompt}
                ]
            }
        ]
        
        # 重试逻辑：最多 3 次
        max_retries = 3
        for retry in range(max_retries):
            try:
                if retry > 0:
                    print(f"   🔄 Retry {retry}/{max_retries}...")
                
                response = self.client.chat.completions.create(
                    model=model if model else "local-model",
                    messages=messages,
                    max_tokens=2048,
                    temperature=0.2
                )
                
                description = response.choices[0].message.content.strip()
                
                if not description:
                    raise ValueError("VLM returned empty description")
                
                word_count = len(description.split())
                print(f"   ✓ Visual description generated ({word_count} words)")
                
                return description
                
            except Exception as e:
                if retry < max_retries - 1:
                    print(f"   ⚠️  Error: {e} - will retry")
                    continue
                else:
                    print(f"   ❌ All retries failed: {e}")
                    print(f"   → Fallback: using basic description")
                    return f"Document page containing text content. Unable to generate detailed visual analysis."
        
        # 不应该到达这里
        return "Document page content analysis unavailable."
    
    def build_prompt(self, ocr_data: Dict[str, Any], page_number: int = 1, region_ocr_data: List[Dict[str, Any]] = None) -> str:
        """构建提示词 - 针对每一页的理解和提取"""
        full_text = ocr_data.get('full_text', '')
        text_blocks_count = ocr_data.get('text_blocks_count', 0)
        avg_confidence = ocr_data.get('average_confidence', 0) * 100
        
        # 构建区域OCR信息（如果有）
        region_info = ""
        if region_ocr_data:
            region_info = "\n\n**Enhanced OCR from High-Resolution Regions (600 DPI):**\n"
            region_info += f"We also performed zoom-in OCR on {len(region_ocr_data)} low-confidence regions at 600 DPI.\n"
            region_info += "These regions had unclear text in the global 300 DPI scan, so we re-scanned them at higher resolution:\n\n"
            
            for i, region in enumerate(region_ocr_data, 1):
                region_text = region.get('full_text', '').strip()
                region_conf = region.get('average_confidence', 0) * 100
                region_bbox = region.get('bbox_300dpi', [0, 0, 0, 0])
                
                if region_text:
                    region_info += f"Region {i} (bbox: {region_bbox}):\n"
                    region_info += f"  Confidence: {region_conf:.1f}%\n"
                    region_info += f"  Text: {region_text[:300]}{'...' if len(region_text) > 300 else ''}\n\n"
            
            region_info += "Note: Use these high-resolution texts as REFERENCE only. Your primary analysis should be based on what YOU SEE in the image.\n"
        
        prompt = f"""You are an expert document analyzer with vision understanding capabilities.

**Task:** Analyze this document page (Page {page_number}) comprehensively - both WHAT YOU SEE in the image and WHAT THE TEXT SAYS.

**OCR Extracted Text (300 DPI Global Scan):**
{full_text}

**OCR Statistics:**
- Text blocks: {text_blocks_count}
- Average confidence: {avg_confidence:.1f}%{region_info}

**Your Analysis Must Include:**

1. **Detailed Visual Description** (CRITICAL for Technical Document Search!)
   **Write a comprehensive 200-400 word description including ALL visible information:**
   
   **A. Visual Structure & Layout:**
   - Page orientation, columns, sections, header/footer, spatial organization
   - Visual elements count & location: Tables (how many, where), diagrams (type, position), charts, photos, stamps, logos, signatures, borders
   - Colors & styling: Dominant colors, highlighting, color-coded elements, backgrounds
   - Text layout: Font hierarchy, density, alignment, headings structure
   
   **B. Technical Details & Content (MUST INCLUDE):**
   - **Component names & part numbers:** "resistor R1", "capacitor C5", "IC chip U2", etc.
   - **Equipment/device IDs:** Model numbers, serial numbers, product codes visible in the page
   - **Technical parameters:** Voltage ratings, dimensions, specifications, measurements
   - **Labels & identifiers:** Any alphanumeric codes, reference designators, catalog numbers
   - **Text content:** Key visible text, titles, headings, annotations, captions
   - **Symbols & notations:** Engineering symbols, mathematical notations, unit symbols
   
   **C. Page Type & Purpose:**
   - What kind of page: title page / data table / circuit diagram / wiring diagram / mechanical drawing / form / specification sheet / mixed
   - Document purpose: Technical manual, design specification, assembly instruction, etc.
   
   **D. Unique Features:**
   - Watermarks, annotations, handwritten notes, stamps (color, position)
   - Quality issues, distinguishing characteristics
   
   **This description will be used for TECHNICAL SEARCHES like:**
   - "Find pages with circuit diagram containing IC U2"
   - "Search for wiring diagrams with connector J1"
   - "Locate specifications for model XYZ-123"
   - "Find pages with red approval stamps"
   
   **CRITICAL: Include ALL visible part numbers, model codes, and technical identifiers!**

2. **Content Understanding**
   - Fix OCR errors (e.g., "4-AU9-25" → "4-Aug-25", "伛 SeP 3" → "15-Sep-25")
   - Clean up garbled text
   - Extract key information based on what's visible

3. **Structured Data Extraction**
   - Document metadata (if visible on this page)
   - Tables (describe structure and content)
   - Technical specifications
   - Any domain-specific information (project, equipment, revisions, etc.)

**Output Format:**
Respond ONLY with a valid JSON object:
```json
{{
  "page_analysis": {{
    "page_number": {page_number},
    "page_type": "title_page | data_table | diagram | text_content | form | mixed",
    "visual_description": "200-400 words COMPLETE description including: 1) Visual layout (structure, colors, element positions, counts); 2) ALL technical details (part numbers like 'R1', 'U2', model codes, device IDs, specifications, measurements); 3) Visible text (titles, labels, annotations, identifiers); 4) Page type & purpose; 5) Unique features (stamps, watermarks, handwritten notes). CRITICAL: Include ALL part numbers, model codes, and technical identifiers visible in the page. This field is used for searching technical documents by visual content AND technical details.",
    "visual_elements": ["table", "diagram", "stamp", "logo", "photo", "chart", "signature", "border", "watermark", "annotation"]
  }},
  
  "extracted_content": {{
    "full_text_cleaned": "Corrected and cleaned text from OCR",
    "key_fields": [
      {{"field": "field_name", "value": "field_value"}}
    ],
    "tables": [
      {{"description": "what this table contains", "rows": 0, "cols": 0}}
    ]
  }},
  
  "document_metadata": {{
    "document_id": "string or null",
    "document_type": "string or null", 
    "revision": "string or null",
    "title": "string or null"
  }},
  
  "domain_specific": {{
    "project": {{"name": "...", "plant": "...", "phase": "..."}} or null,
    "equipment": {{"tag": "...", "name": "...", "unit": "..."}} or null,
    "revisions": [...] or null
  }},
  
  "keywords": ["keyword1", "keyword2"],
  "confidence": 0.0-1.0,
  "notes": ["any uncertainties or observations"]
}}
```

**Critical Requirements:** 
- The visual_description MUST be 200-400 words and describe EVERYTHING visible
- MUST include ALL technical identifiers: part numbers (R1, C5, U2), model codes, device IDs, specifications
- This field is used for searching technical documents (circuit diagrams, engineering drawings, specifications)
- Include both VISUAL features (layout, colors, element positions) AND TECHNICAL content (part names, numbers, parameters)
- Be specific about positions, colors, counts, and distinguishing characteristics
- DO NOT just repeat OCR text - synthesize visual AND technical information into a comprehensive description

Respond with ONLY the JSON, no additional text."""

        return prompt
    
    def refine_with_image(self, image_path: str, ocr_json_path: str, 
                          model: str = None, page_number: int = 1, 
                          region_ocr_results: List[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        使用两次独立的 VLM 调用处理页面
        
        Args:
            image_path: 图片路径
            ocr_json_path: OCR结果JSON路径
            model: 模型名称
            page_number: 页码
            region_ocr_results: 区域OCR结果（当前未使用）
            
        Returns:
            完整的结构化文档数据
        """
        print(f"\n📄 Processing Page {page_number}: {os.path.basename(image_path)}")
        print("=" * 80)
        
        # 读取 OCR 数据
        with open(ocr_json_path, 'r', encoding='utf-8') as f:
            ocr_data = json.load(f)
        
        ocr_text = ocr_data.get('full_text', '')
        avg_confidence = ocr_data.get('average_confidence', 0.0)
        text_blocks = ocr_data.get('text_blocks', [])
        
        # ============ 第一次调用：生成清理后的文本 ============
        cleaned_text = self.generate_cleaned_text(image_path, ocr_text, model)
        
        # ============ 第二次调用：生成视觉描述 ============
        visual_description = self.generate_visual_description(image_path, cleaned_text, model)
        
        # ============ 在代码中构建完整的 JSON 结构 ============
        print("\n🏗️  Building complete document structure...")
        
        # 从 visual_description 中简单提取 visual_elements（关键词匹配）
        description_lower = visual_description.lower()
        visual_elements = []
        element_keywords = {
            'table': ['table', '表格', 'grid'],
            'diagram': ['diagram', '图', 'circuit', 'wiring', 'schematic'],
            'stamp': ['stamp', '印章', 'seal', 'approval'],
            'logo': ['logo', '标志'],
            'photo': ['photo', '照片', 'image', 'picture'],
            'chart': ['chart', '图表', 'graph'],
            'signature': ['signature', '签名', 'signed'],
            'annotation': ['annotation', '注释', 'handwritten', 'note'],
            'border': ['border', '边框', 'frame'],
            'watermark': ['watermark', '水印']
        }
        
        for element, keywords in element_keywords.items():
            if any(kw in description_lower for kw in keywords):
                visual_elements.append(element)
        
        # 构建完整结构（只有两个字段来自 VLM，其他全部由代码填充）
        complete_doc = {
            "page_analysis": {
                "page_number": page_number,  # 代码填充
                "page_type": "mixed",  # 代码填充（固定值）
                "visual_description": visual_description,  # ⭐ VLM 第二次调用
                "visual_elements": visual_elements  # 代码提取
            },
            
            "extracted_content": {
                "full_text_cleaned": cleaned_text,  # ⭐ VLM 第一次调用
                "key_fields": [],  # 代码填充（留空）
                "tables": []  # 代码填充（留空）
            },
            
            "document_metadata": {
                "document_id": None,  # 代码填充
                "document_type": None,
                "revision": None,
                "title": None
            },
            
            "domain_specific": None,  # 代码填充
            
            "keywords": [],  # 弃用，留空
            
            "confidence": avg_confidence,  # 从 OCR 数据获取
            
            "notes": []  # 弃用，留空
        }
        
        print("✅ Document structure built successfully")
        print(f"   - Cleaned text: {len(cleaned_text)} chars")
        print(f"   - Visual description: {len(visual_description.split())} words")
        print(f"   - Visual elements detected: {len(visual_elements)}")
        print("=" * 80)
        
        return complete_doc
    
    def refine_text_only(self, ocr_data: Dict[str, Any], model: str = None) -> Dict[str, Any]:
        """
        仅使用文本模式优化OCR结果（当vision不可用时）
        
        Args:
            ocr_data: OCR结果数据
            model: 模型名称
            
        Returns:
            精炼后的结构化数据
        """
        print("📝 Using text-only mode...")
        
        prompt = self.build_prompt(ocr_data)
        
        try:
            response = self.client.chat.completions.create(
                model=model if model else "local-model",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=4096,
                temperature=0.1,
            )
            
            content = response.choices[0].message.content
            
            # 解析JSON（使用增强的鲁棒性解析）
            json_start = content.find("{")
            json_end = content.rfind("}") + 1
            
            if json_start != -1 and json_end > json_start:
                json_str = content[json_start:json_end]
                
                # 尝试多种解析策略
                for attempt in range(3):
                    try:
                        if attempt == 0:
                            refined_data = json.loads(json_str)
                        elif attempt == 1:
                            import re
                            fixed_json = re.sub(r'\\(?!["\\/bfnrtu])', r'\\\\', json_str)
                            refined_data = json.loads(fixed_json)
                            print("   ℹ️  Fixed invalid escape sequences (text-only mode)")
                        elif attempt == 2:
                            refined_data = json.loads(json_str, strict=False)
                            print("   ℹ️  Parsed with strict=False mode (text-only)")
                        
                        return refined_data
                        
                    except json.JSONDecodeError as e:
                        if attempt < 2:
                            continue
                        else:
                            raise ValueError(f"Failed to parse JSON after {attempt+1} attempts: {e}")
            else:
                raise ValueError("No valid JSON found in model response")
            
        except Exception as e:
            print(f"❌ Text-only refinement failed: {e}")
            # 返回基础结构
            return {
                "document_metadata": {},
                "document_content": {},
                "revision_history": [],
                "procedures": {},
                "keywords": [],
                "full_text_cleaned": ocr_data.get('full_text', ''),
                "extraction_notes": [f"Error during refinement: {str(e)}"]
            }
    
    def create_page_vlm_document(self, refined_data: Dict[str, Any], 
                                  ocr_data: Dict[str, Any],
                                  image_path: str, page_number: int) -> Dict[str, Any]:
        """
        创建包含OCR和VLM结果的完整页面文档
        
        Args:
            refined_data: VLM精炼后的数据
            ocr_data: 原始OCR数据
            image_path: 图片路径
            page_number: 页码
            
        Returns:
            完整的页面文档结构
        """
        # 提取VLM分析结果
        page_analysis = refined_data.get('page_analysis', {})
        extracted_content = refined_data.get('extracted_content', {})
        doc_metadata = refined_data.get('document_metadata', {})
        domain_specific = refined_data.get('domain_specific', {})
        
        # 构建完整页面文档
        page_doc = {
            # ===== 页面基础信息 =====
            "page_number": page_number,
            "image_path": os.path.abspath(image_path),
            "image_filename": os.path.basename(image_path),
            
            # ===== VLM页面分析（新增！）=====
            "page_analysis": {
                "page_type": page_analysis.get('page_type', 'unknown'),
                # New format: visual_description (primary field for semantic search)
                "visual_description": page_analysis.get('visual_description', ''),
                # Legacy format fields (keep for backward compatibility)
                "page_description": page_analysis.get('page_description', ''),
                "visual_elements": page_analysis.get('visual_elements', []),
                "layout_structure": page_analysis.get('layout_structure', '')
            },
            
            # ===== 文本内容 =====
            "content": {
                "full_text_raw": ocr_data.get('full_text', ''),
                "full_text_cleaned": extracted_content.get('full_text_cleaned', ''),
                "key_fields": extracted_content.get('key_fields', []),
                "tables": extracted_content.get('tables', [])
            },
            
            # ===== 原始OCR数据 =====
            "ocr_data": {
                "text_blocks": ocr_data.get('text_blocks', []),
                "text_blocks_count": ocr_data.get('text_blocks_count', 0),
                "average_confidence": ocr_data.get('average_confidence', 0),
                "image_size": ocr_data.get('image_size', {}),
                "layout_regions": ocr_data.get('layout_regions', [])
            },
            
            # ===== 提取的元数据 =====
            "metadata": doc_metadata,
            
            # ===== 领域特定信息 =====
            "domain_data": domain_specific,
            
            # ===== 搜索关键词 =====
            "keywords": refined_data.get('keywords', []),
            
            # ===== VLM置信度和注释 =====
            "vlm_metadata": {
                "confidence": refined_data.get('confidence', 0.0),
                "extraction_notes": refined_data.get('notes', [])
            }
        }
        
        return page_doc


def main():
    # Load default configuration from config.yaml
    try:
        import sys
        from pathlib import Path
        # Add parent directory to path to import config
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from src.config import config
        vision_cfg = config.vision_config
        default_api_base = vision_cfg.get('api_url', 'http://localhost:1234/v1')
        default_model = vision_cfg.get('model_name', 'google/gemma-3-27b')
        config_loaded = True
    except ImportError:
        default_api_base = 'http://localhost:1234/v1'
        default_model = 'google/gemma-3-27b'
        config_loaded = False
    
    parser = argparse.ArgumentParser(
        description="使用VLM模型优化OCR结果，生成ES友好的JSON"
    )
    parser.add_argument("image", help="图片文件路径")
    parser.add_argument("ocr_json", help="OCR结果JSON路径")
    parser.add_argument("-o", "--output", help="输出JSON路径（默认：xxx_vlm.json）")
    parser.add_argument("-p", "--page-number", type=int, default=1, 
                       help="页码（用于VLM理解，默认：1）")
    parser.add_argument("-r", "--regions-json", 
                       help="阶段3区域OCR结果JSON路径（可选）")
    parser.add_argument("--api-base", default=default_api_base,
                       help=f"LM Studio API地址（默认从config: {default_api_base}）")
    parser.add_argument("--model", default=default_model,
                       help=f"模型名称（默认从config: {default_model}）")
    parser.add_argument("--text-only", action="store_true",
                       help="仅使用文本模式（不发送图片）")
    parser.add_argument("--pretty", action="store_true",
                       help="输出可读性格式")
    
    args = parser.parse_args()
    
    # 检查文件是否存在
    image_path = Path(args.image)
    ocr_json_path = Path(args.ocr_json)
    
    if not image_path.exists():
        print(f"❌ Error: Image not found: {image_path}")
        return 1
    
    if not ocr_json_path.exists():
        print(f"❌ Error: OCR JSON not found: {ocr_json_path}")
        return 1
    
    # 确定输出路径
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = image_path.with_stem(image_path.stem + "_vlm").with_suffix('.json')
    
    print("="*80)
    print(f"🚀 VLM Page Analysis (Page {args.page_number})")
    print("="*80)
    if config_loaded:
        print(f"✓ Configuration loaded from config.yaml")
    else:
        print(f"⚠ Using default configuration (config.yaml not found)")
    print(f"Model: {args.model}")
    print(f"API: {args.api_base}")
    print("="*80)
    
    try:
        # 初始化精炼器
        refiner = VLMRefiner(api_base=args.api_base)
        
        # 读取OCR数据
        with open(ocr_json_path, 'r', encoding='utf-8') as f:
            ocr_data = json.load(f)
        
        # 读取区域OCR数据（如果提供）
        region_ocr_results = None
        if args.regions_json:
            regions_path = Path(args.regions_json)
            if regions_path.exists():
                with open(regions_path, 'r', encoding='utf-8') as f:
                    region_ocr_results = json.load(f)
                print(f"✓ Loaded {len(region_ocr_results)} region OCR results")
            else:
                print(f"⚠ Warning: Regions JSON not found: {regions_path}")
        
        # 精炼数据
        if args.text_only:
            refined_data = refiner.refine_text_only(ocr_data, args.model)
        else:
            refined_data = refiner.refine_with_image(
                str(image_path), 
                str(ocr_json_path),
                args.model,
                args.page_number,
                region_ocr_results
            )
        
        print("\n✓ VLM analysis completed")
        
        # 创建完整页面文档
        print("📦 Creating complete page document...")
        page_doc = refiner.create_page_vlm_document(
            refined_data, ocr_data, str(image_path), args.page_number
        )
        
        # 保存结果
        print(f"💾 Saving to: {output_path}")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(page_doc, f, ensure_ascii=False, indent=2 if args.pretty else None)
        
        print("\n" + "="*80)
        print("✅ SUCCESS!")
        print("="*80)
        
        # 打印摘要
        if args.pretty:
            print("\n📋 Page Analysis Summary:")
            print(f"  Page Number: {page_doc.get('page_number', 'N/A')}")
            print(f"  Page Type: {page_doc.get('page_analysis', {}).get('page_type', 'N/A')}")
            print(f"  Description: {page_doc.get('page_analysis', {}).get('page_description', 'N/A')[:100]}...")
            print(f"  Visual Elements: {', '.join(page_doc.get('page_analysis', {}).get('visual_elements', []))}")
            print(f"  Keywords: {', '.join(page_doc.get('keywords', [])[:5])}")
            print(f"  OCR Confidence: {page_doc.get('ocr_data', {}).get('average_confidence', 0):.2f}")
            print(f"  VLM Confidence: {page_doc.get('vlm_metadata', {}).get('confidence', 0):.2f}")
        
        print(f"\n📁 Output: {output_path}")
        return 0
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

