#!/usr/bin/env python3
"""
PDF 文档智能 OCR 处理脚本（支持 VLM 修正）
在原有 adaptive_ocr_pipeline.py 基础上增加 VLM 智能修正
"""

import argparse
import json
import sys
import subprocess
from pathlib import Path
from typing import Dict, Any, Tuple
import structlog
import pdfplumber

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config import config

# 初始化日志
logger = structlog.get_logger(__name__)

# 尝试导入 VLM
HAS_VLM = False
try:
    from src.models import VisionModel
    HAS_VLM = True
except Exception as e:
    logger.warning(f"VLM not available: {e}")


def should_use_vlm_refinement(ocr_data: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
    """
    判断是否需要 VLM 修正
    
    Args:
        ocr_data: OCR 原始结果
    
    Returns:
        (是否需要修正, 原因, 统计信息)
    """
    text_blocks = ocr_data.get('text_blocks', [])
    
    if not text_blocks:
        return False, "无文本内容", {
            'avg_confidence': 0.0,
            'garbled_ratio': 0.0,
            'total_blocks': 0,
            'total_chars': 0
        }
    
    # 统计分析
    confidences = [b.get('confidence', 0) for b in text_blocks if b.get('confidence', 0) > 0]
    avg_confidence = sum(confidences) / len(confidences) if confidences else 0.0
    
    # 检测乱码字符
    all_text = ' '.join([b.get('text', '') for b in text_blocks])
    garbled_chars = sum(1 for c in all_text if ord(c) > 0x4E00 and c in '�□▪︎◆■●○◇')
    garbled_ratio = garbled_chars / len(all_text) if all_text else 0.0
    
    # 检测 URL 片段（可能识别错误）
    has_url_fragments = any(pattern in all_text.lower() for pattern in ['http', 'www.', '.com', '://', 'https'])
    
    # 检测文件列表模式
    lines = [b.get('text', '').strip() for b in text_blocks if b.get('text', '').strip()]
    short_lines = sum(1 for line in lines if len(line) < 50)
    is_file_list = (
        short_lines > 5 and 
        short_lines / len(lines) > 0.6 if lines else False
    ) or any(ext in all_text.lower() for ext in ['.tar', '.dmg', '.pkg', '.pdf', '.docx'])
    
    # 检测多行短文本（可能是列表/目录）
    is_multi_short_lines = len(lines) >= 5 and short_lines / len(lines) > 0.7 if lines else False
    
    # 检测思维导图/关系图（树形符号密度）
    tree_symbols = sum(all_text.count(s) for s in ['├', '└', '│', '──', '─'])
    arrow_symbols = sum(all_text.count(s) for s in ['→', '←', '↓', '↑', '⇒', '⇐', '▶', '◀'])
    is_mindmap = (tree_symbols > 5 or arrow_symbols > 3) and len(text_blocks) > 8
    
    stats = {
        'avg_confidence': avg_confidence,
        'garbled_ratio': garbled_ratio,
        'is_file_list': is_file_list,
        'is_multi_short_lines': is_multi_short_lines,
        'has_url_fragments': has_url_fragments,
        'is_mindmap': is_mindmap,
        'tree_symbols_count': tree_symbols,
        'arrow_symbols_count': arrow_symbols,
        'total_blocks': len(text_blocks),
        'total_chars': len(all_text)
    }
    
    # 宽松介入策略
    if avg_confidence < 0.8:  # 80% 以下就修正
        return True, f"识别质量可提升 (置信度 {avg_confidence:.1%})", stats
    elif garbled_ratio > 0.005:  # 0.5% 乱码即触发
        return True, f"检测到乱码 ({garbled_ratio:.1%})", stats
    elif stats.get('is_mindmap', False):  # 思维导图
        return True, "检测到思维导图/关系图", stats
    elif has_url_fragments:  # URL 可能识别错误
        return True, "检测到 URL，需要修正", stats
    elif is_file_list or is_multi_short_lines:  # 特殊格式
        return True, "检测到列表结构", stats
    
    return False, "质量良好", stats


def refine_text_with_vlm(
    image_path: Path,
    ocr_text: str,
    vlm_model,
    confidence_info: Dict[str, Any] = None
) -> str:
    """
    使用 VLM 修正 OCR 文本
    
    Args:
        image_path: 图片路径
        ocr_text: OCR 原始文本
        vlm_model: VisionModel 实例
        confidence_info: 置信度信息
    
    Returns:
        修正后的文本
    """
    if not HAS_VLM or not vlm_model:
        return ocr_text
    
    try:
        # 构建质量提示信息
        quality_note = ""
        context_hint = ""
        correction_level = ""
        
        if confidence_info:
            avg_conf = confidence_info.get('avg_confidence', 0)
            garbled_ratio = confidence_info.get('garbled_ratio', 0)
            has_url = confidence_info.get('has_url_fragments', False)
            is_file_list = confidence_info.get('is_file_list', False)
            
            if avg_conf < 0.5:
                quality_note = f"\n注意：OCR 识别质量较低（平均置信度 {avg_conf:.1%}），可能存在较多错误。"
                correction_level = "【激进修正模式】识别质量很低，需要大幅修正错别字和结构"
            elif avg_conf < 0.7:
                quality_note = f"\n注意：OCR 识别质量中等（平均置信度 {avg_conf:.1%}），可能有错误。"
                correction_level = "【中等修正模式】适度修正明显的错别字，保留大部分原文"
            elif avg_conf < 0.8:
                quality_note = f"\n注意：OCR 识别质量尚可（平均置信度 {avg_conf:.1%}）。"
                correction_level = "【保守修正模式】仅修正明显错误，保留格式和边距"
            
            if garbled_ratio > 0.005:
                quality_note += f"\n注意：检测到 {garbled_ratio:.1%} 的乱码字符，请参考图片修正。"
            if has_url:
                context_hint = "这是包含 URL 链接的内容，请确保 URL 格式正确"
            elif is_file_list:
                context_hint = "这是一个文件列表/目录"
        
        prompt = f"""请根据图片和 OCR 识别结果，修正以下文本中的错误：

OCR 原始结果：
{ocr_text}

识别质量信息：
{quality_note}
{correction_level}

修正要求：
1. **错别字修正**（必须参考图片）：
   - 容器监控/应用监控/数据库监控 等IT术语
   - 常见错误：客器→容器、申间→空间、V志→日志、禺→域
   - 专有名词：CyberArk、Kong、API Gateway、CMDB
   - URL 链接格式：http/https, ://, 域名

2. **格式保留**（禁止修改）：
   - 树形符号：├ │ └ ── 
   - 缩进层级：必须与原文一致
   - 换行位置：保持原有布局

3. **结构修复**：
   - 补充丢失的符号（/, -, |, ├, └）
   - 恢复文件/文件夹层级关系
   - 修正 URL 断行/空格错误
   - 合并被错误分割的词语

4. **禁止行为**：
   - 不要添加原图中没有的内容
   - 不要改变技术术语的含义
   - 不要删除看似重复但实际存在的内容

{f'提示：{context_hint}' if context_hint else ''}

请直接返回修正后的文本内容，不要有其他解释。"""

        logger.info("🤖 调用 VLM 修正文本...",
                   image=str(image_path.name),
                   ocr_length=len(ocr_text),
                   avg_confidence=confidence_info.get('avg_confidence', 0) if confidence_info else 0)
        
        response = vlm_model.extract_text_from_image(str(image_path), prompt)
        refined_text = response.get('text', ocr_text)
        
        # 基本验证：防止 VLM 幻觉或截断
        if len(refined_text) < len(ocr_text) * 0.3 or len(refined_text) > len(ocr_text) * 5:
            logger.warning("⚠️  VLM 修正结果长度异常，使用原始 OCR",
                          original_len=len(ocr_text),
                          refined_len=len(refined_text))
            return ocr_text
        
        logger.info("✅ VLM 修正完成",
                   original_len=len(ocr_text),
                   refined_len=len(refined_text),
                   change_ratio=f"{(len(refined_text)/len(ocr_text)-1)*100:+.1f}%")
        
        return refined_text
        
    except Exception as e:
        logger.error(f"❌ VLM 修正失败: {e}", image_path=str(image_path))
        return ocr_text


def process_pdf_page_with_vlm(
    page_image_path: Path,
    ocr_json_path: Path,
    output_dir: Path,
    vlm_model = None
) -> Dict[str, Any]:
    """
    对单个 PDF 页面的 OCR 结果应用 VLM 修正
    
    Args:
        page_image_path: 页面图片路径
        ocr_json_path: OCR JSON 结果路径
        output_dir: 输出目录
        vlm_model: VisionModel 实例
    
    Returns:
        修正结果
    """
    # 读取 OCR 结果
    with open(ocr_json_path, 'r', encoding='utf-8') as f:
        ocr_data = json.load(f)
    
    text_blocks = ocr_data.get('text_blocks', [])
    original_text = ocr_data.get('text', '')
    if not original_text and text_blocks:
        original_text = '\n'.join([b.get('text', '') for b in text_blocks if b.get('text')])
    
    # 判断是否需要 VLM 修正
    need_vlm, reason, stats = should_use_vlm_refinement(ocr_data)
    
    logger.info(f"  🎯 质量分析:", **stats)
    logger.info(f"  {'✅' if need_vlm else '❌'} VLM 修正: {reason}")
    
    final_text = original_text
    vlm_refined = False
    
    if need_vlm and HAS_VLM and vlm_model:
        confidence_info = {
            'avg_confidence': stats['avg_confidence'],
            'garbled_ratio': stats['garbled_ratio'],
            'has_url_fragments': stats.get('has_url_fragments', False),
            'is_file_list': stats.get('is_file_list', False)
        }
        
        final_text = refine_text_with_vlm(
            image_path=page_image_path,
            ocr_text=original_text,
            vlm_model=vlm_model,
            confidence_info=confidence_info
        )
        
        if final_text != original_text:
            vlm_refined = True
            logger.info("  ✅ VLM 修正完成",
                       original_len=len(original_text),
                       refined_len=len(final_text))
    
    # Try to load VLM JSON data (which contains page_analysis)
    page_number = page_image_path.stem.split('_')[1]  # Extract page number from filename
    vlm_json_path = output_dir / f"page_{page_number}_vlm.json"
    vlm_json_data = None
    
    if vlm_json_path.exists():
        try:
            with open(vlm_json_path, 'r', encoding='utf-8') as f:
                vlm_json_data = json.load(f)
        except Exception as e:
            logger.warning(f"  ⚠️  Failed to load VLM JSON: {e}")
    
    return {
        'text': final_text,
        'vlm_refined': vlm_refined,
        'stats': stats,
        'reason': reason,
        'vlm_json': vlm_json_data  # Include full VLM JSON data
    }


def main():
    parser = argparse.ArgumentParser(description='PDF 文档智能 OCR 处理（支持 VLM 修正）')
    parser.add_argument('pdf_path', type=str, help='PDF 文件路径')
    parser.add_argument('--ocr-engine', type=str, default='vision',
                       choices=['vision', 'paddle', 'easy'],
                       help='OCR 引擎选择 (默认: vision)')
    parser.add_argument('--output-dir', type=str, default=None,
                       help='输出目录（默认：PDF名_adaptive）')
    parser.add_argument('--processing-mode', type=str, default='fast',
                       choices=['fast', 'deep'],
                       help='处理模式: fast=快速(OCR+VLM一次处理), deep=深度(完整4阶段处理，默认: fast)')
    
    args = parser.parse_args()
    
    pdf_path = Path(args.pdf_path)
    if not pdf_path.exists():
        print(f"❌ PDF 不存在: {pdf_path}")
        sys.exit(1)
    
    # 确定输出目录（与 adaptive_ocr_pipeline.py 保持一致）
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        # 使用与 adaptive_ocr_pipeline.py 相同的命名规则
        output_dir = Path(pdf_path.stem.replace(' ', '_') + "_adaptive")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info("=" * 80)
    logger.info("📄 开始处理 PDF 文档（智能 VLM 模式）", pdf=pdf_path.name, ocr_engine=args.ocr_engine)
    logger.info("=" * 80)
    
    # 初始化 VLM
    vlm_model = None
    if HAS_VLM:
        try:
            vlm_config = config.vision_config
            if vlm_config.get('enabled', False):
                vlm_model = VisionModel(vlm_config)
                logger.info("✅ VLM 已启用")
        except Exception as e:
            logger.warning(f"⚠️  VLM 初始化失败: {e}")
    
    # 先调用原有的 adaptive_ocr_pipeline 生成基础 OCR
    logger.info("📍 阶段 1: 运行 Adaptive OCR Pipeline...")
    
    # 调用 adaptive_ocr_pipeline.py 作为子进程
    import subprocess
    adaptive_script = Path('document_ocr_pipeline/adaptive_ocr_pipeline.py')
    subprocess.run([
        sys.executable,
        str(adaptive_script),
        str(pdf_path),
        '--ocr-engine', args.ocr_engine,
        '--output-dir', str(output_dir),
        '--processing-mode', args.processing_mode
    ], check=True, cwd=project_root)
    
    # 读取生成的 complete_adaptive_ocr.json
    complete_json = output_dir / "complete_adaptive_ocr.json"
    if not complete_json.exists():
        logger.error("❌ Adaptive OCR 输出未找到")
        sys.exit(1)
    
    with open(complete_json, 'r', encoding='utf-8') as f:
        adaptive_data = json.load(f)
    
    logger.info("📍 阶段 2: VLM 智能修正...")
    
    # 对每一页应用 VLM 修正
    pages = adaptive_data.get('pages', [])
    pages_for_index = []
    
    for page in pages:
        page_num = page.get('page_number')
        logger.info(f"  📄 处理第 {page_num} 页...")
        
        # 获取页面图片和 OCR JSON
        stage1 = page.get('stage1_global', {})
        image_filename = stage1.get('image', f'page_{page_num:03d}_300dpi.png')
        ocr_json_filename = stage1.get('ocr_json', f'page_{page_num:03d}_global_ocr.json')
        
        page_image_path = output_dir / image_filename
        ocr_json_path = output_dir / ocr_json_filename
        
        if not page_image_path.exists() or not ocr_json_path.exists():
            logger.warning(f"  ⚠️  页面文件缺失，跳过")
            continue
        
        # VLM 修正
        vlm_result = process_pdf_page_with_vlm(
            page_image_path=page_image_path,
            ocr_json_path=ocr_json_path,
            output_dir=output_dir,
            vlm_model=vlm_model
        )
        
        # 更新页面统计信息
        if 'statistics' not in page:
            page['statistics'] = {}
        
        page['statistics']['avg_ocr_confidence'] = vlm_result['stats'].get('avg_confidence', 0.0)
        page['statistics']['vlm_refined'] = vlm_result['vlm_refined']
        
        # 构建索引文档（标准格式，兼容 document_processor.py）
        page_data = {
            'page_number': page_num,
            'image_path': str(page_image_path),
            'image_filename': image_filename,
            'content': {
                'full_text_cleaned': vlm_result['text'],
                'full_text_raw': vlm_result['text'],
                'key_fields': [],
                'tables': []
            },
            'ocr_data': {
                'text_blocks': []  # 可后续补充
            },
            'metadata': {
                'extraction_method': 'ocr_vlm_refined' if vlm_result['vlm_refined'] else 'ocr',
                'ocr_engine': args.ocr_engine,
                'avg_ocr_confidence': vlm_result['stats'].get('avg_confidence', 0.0),
                'vlm_refined': vlm_result['vlm_refined']
            }
        }
        
        # Include page_analysis from VLM JSON if available
        if vlm_result.get('vlm_json') and 'page_analysis' in vlm_result['vlm_json']:
            page_data['page_analysis'] = vlm_result['vlm_json']['page_analysis']
            logger.info(f"  ✅ 包含 page_analysis 数据")
        
        pages_for_index.append(page_data)
    
    # 保存更新后的 complete_adaptive_ocr.json
    with open(complete_json, 'w', encoding='utf-8') as f:
        json.dump(adaptive_data, f, ensure_ascii=False, indent=2)
    
    # 保存可搜索文本（用于 ES 索引）
    complete_document_path = output_dir / "complete_document.json"
    with open(complete_document_path, 'w', encoding='utf-8') as f:
        json.dump({'pages': pages_for_index}, f, ensure_ascii=False, indent=2)
    
    logger.info("=" * 80)
    logger.info("🎉 PDF 处理完成!")
    logger.info(f"  📊 总页数: {len(pages)}")
    vlm_count = sum(1 for p in pages_for_index if p.get('extraction_method') == 'ocr_vlm_refined')
    logger.info(f"  🤖 VLM 修正: {vlm_count}/{len(pages)} 页")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()

