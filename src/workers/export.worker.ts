// src/workers/export.worker.ts
// This worker is responsible for generating the Excel file on the client-side.

import ExcelJS from "exceljs";
import type { AuditFinding } from "@/lib/auditEngine";
import type { RecordRow } from "@/lib/types";
import { generateArabicClusterSummary, getDecisionAndNote } from '@/lib/arabicClusterSummary';
import { computePairScore } from "@/lib/scoringClient";
import { calculateClusterConfidence } from "@/lib/clusterConfidence";

/**
 * Safely posts a message from the worker, using structuredClone to verify serializability.
 * This prevents "DataCloneError" for non-transferable objects.
 * @param {any} message - The message to send to the main thread.
 */
function safePostMessage(message: any) {
  try {
    structuredClone(message);
    postMessage(message);
  } catch {
    postMessage({
      type: 'error',
      data: 'Worker message serialization failed'
    });
  }
}

function fullPairwiseBreakdown(cluster: RecordRow[]) {
    const pairs: { a: RecordRow; b: RecordRow; score: number; breakdown: any }[] = [];
    if (cluster.length < 2) return pairs;

    for (let i = 0; i < cluster.length; i++) {
        for (let j = i + 1; j < cluster.length; j++) {
            const result = computePairScore(cluster[i], cluster[j], {});
            pairs.push({
                a: cluster[i],
                b: cluster[j],
                score: result.score,
                breakdown: result.breakdown
            });
        }
    }
    return pairs;
}

type EnrichedRecord = RecordRow & {
    ClusterID?: number | null; // Internal sequential ID
    Generated_Cluster_ID?: number | null; // The one for display, avoids conflict
    Cluster_Size?: number | null;
    Flag?: string | null;
    Max_PairScore?: number | null;
    pairScore?: number;
    nameScore?: number;
    husbandScore?: number;
    childrenScore?: number;
    idScore?: number;
    phoneScore?: number;
    locationScore?: number;
    'تصنيف المجموعة المبدئي'?: string;
    'نتائج تحليل المجموعة'?: string;
    [key: string]: any;
};

// Main message handler for the worker
self.onmessage = async (event) => {
    try {
        const { cachedData } = event.data;
        if (!cachedData) {
            throw new Error("Cached data is required for export.");
        }
        
        safePostMessage({ type: 'progress', step: 'enriching', progress: 10 });
        const { enrichedRecords, enrichedClusters } = await enrichData(cachedData);
        
        safePostMessage({ type: 'progress', step: 'sorting', progress: 30 });
        const sortedData = sortData(enrichedRecords);
        
        safePostMessage({ type: 'progress', step: 'sheets', progress: 50 });
        const wb = createFormattedWorkbook(sortedData, cachedData, enrichedClusters);

        const buffer = await wb.xlsx.writeBuffer();

        // ExcelJS may return Uint8Array in some builds
        const arrayBuffer =
        buffer instanceof ArrayBuffer
            ? buffer
            : buffer.buffer;

        safePostMessage({ type: 'progress', step: 'done', progress: 100 });

        // Send ONLY the transferable ArrayBuffer
        self.postMessage(
        { type: 'done', data: arrayBuffer },
        [arrayBuffer]
        );
        
    } catch (error: any) {
        safePostMessage({ type: 'error', data: error instanceof Error ? error.message : String(error) });
    }
};


async function enrichData(cachedData: any): Promise<{ enrichedRecords: EnrichedRecord[], enrichedClusters: any[] }> {
    const { rows: allRecords, clusters } = cachedData;
    if (!allRecords || !clusters) {
        throw new Error("Invalid cache: missing rows or clusters.");
    }

    const enrichedClusters = clusters.map((clusterObj: any) => {
        const pairs = clusterObj.pairScores || [];
        const finalScores = pairs.map((p: any) => p.score || 0);

        return {
            ...clusterObj,
            Max_PairScore: Math.max(...finalScores, 0),
            size: clusterObj.records.length,
            generatedClusterId: clusterObj.records.reduce((max: number, record: RecordRow) => {
                const currentId = Number(record.beneficiaryId);
                return !isNaN(currentId) && currentId > max ? currentId : max;
            }, 0) || (clusters.indexOf(clusterObj) + 1),
        };
    });

    const recordToCluster = new Map<string, any>();
    enrichedClusters.forEach((c: any) => {
        c.records.forEach((r: RecordRow) => {
            recordToCluster.set(r._internalId!, c);
        });
    });

    const enrichedRecords: EnrichedRecord[] = allRecords.map((record: RecordRow) => {
        const cluster = recordToCluster.get(record._internalId!);
        if (cluster) {
            const { decision, expertNote } = getDecisionAndNote(cluster.confidenceScore || 0);
            
            const score = cluster.avgFinalScore * 100;
            let flag = '?';
            if (score >= 90) flag = 'm?';
            else if (score >= 80) flag = 'm';
            else if (score >= 70) flag = '??';
            
            return {
                ...record,
                ClusterID: enrichedClusters.indexOf(cluster) + 1,
                Generated_Cluster_ID: cluster.generatedClusterId,
                Flag: flag,
                Cluster_Size: cluster.size,
                Max_PairScore: cluster.Max_PairScore,
                'تصنيف المجموعة المبدئي': decision,
                'نتائج تحليل المجموعة': expertNote,
                pairScore: cluster.avgFinalScore,
                nameScore: record.nameScore,
                husbandScore: record.husbandScore,
                childrenScore: record.childrenScore,
                idScore: record.idScore,
                phoneScore: record.phoneScore,
                locationScore: record.locationScore,
            };
        }
        return record;
    });

    return { enrichedRecords, enrichedClusters };
}

function sortData(data: EnrichedRecord[]): EnrichedRecord[] {
    return data.sort((a, b) => {
        const scoreA = a.Max_PairScore ?? -1;
        const scoreB = b.Max_PairScore ?? -1;
        if (scoreA !== scoreB) {
            return scoreB - scoreA;
        }

        const clusterA = a.Generated_Cluster_ID ?? Number.MAX_SAFE_INTEGER;
        const clusterB = b.Generated_Cluster_ID ?? Number.MAX_SAFE_INTEGER;
        if (clusterA !== clusterB) {
            return clusterA - clusterB;
        }
        
        return String(a.beneficiaryId || '').localeCompare(String(b.beneficiaryId || ''));
    });
}

function createFormattedWorkbook(data: EnrichedRecord[], cachedData: any, enrichedClusters: any[]): ExcelJS.Workbook {
    const { rows: allRecords, auditFindings, originalHeaders, chartImages, processedDataForReport } = cachedData;
    const wb = new ExcelJS.Workbook();
    wb.creator = "Beneficiary Insights";
    
    safePostMessage({ type: 'progress', step: 'sheets', progress: 60 });
    createEnrichedDataSheet(wb, data, originalHeaders);
    
    safePostMessage({ type: 'progress', step: 'summary', progress: 75 });
    createSummarySheet(wb, allRecords, enrichedClusters, auditFindings || []);
    
    if (auditFindings && auditFindings.length > 0) {
        safePostMessage({ type: 'progress', step: 'audit', progress: 85 });
        createAuditSheet(wb, auditFindings, enrichedClusters);
    }
    createClustersSheet(wb, enrichedClusters);
    
    if (chartImages && processedDataForReport) {
        safePostMessage({ type: 'progress', step: 'dashboard', progress: 95 });
        createDashboardReportSheet(wb, chartImages, processedDataForReport, self);
    }

    return wb;
}


// ===============================================
// EXCEL SHEET GENERATION FUNCTIONS
// ===============================================

function createEnrichedDataSheet(wb: ExcelJS.Workbook, data: EnrichedRecord[], originalHeaders: string[]) {
    const ws = wb.addWorksheet("Enriched Data");
    ws.views = [{ rightToLeft: true }];
    
    const enrichmentHeaders = [
        "Generated_Cluster_ID", "Cluster_Size", "Flag", "Max_PairScore",
        "pairScore", "nameScore", "husbandScore", "childrenScore", "idScore", "phoneScore", "locationScore",
        "تصنيف المجموعة المبدئي", "نتائج تحليل المجموعة"
    ];
    
    const finalOriginalHeaders = originalHeaders.filter(h => !h.startsWith('_'));
    const finalHeaders = [ ...enrichmentHeaders, ...finalOriginalHeaders ];
    
    ws.columns = finalHeaders.map(h => ({
      header: h,
      key: h,
      width: h === 'womanName' || h === 'husbandName' ? 25 : (h === 'نتائج تحليل المجموعة' ? 50 : 15)
    }));

    ws.getRow(1).eachCell(cell => {
        cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF002060' } };
        cell.font = { color: { argb: 'FFFFFFFF' }, bold: true };
        cell.alignment = { horizontal: 'center' };
    });
    
    const dataForSheet = data.map(record => {
        const newRecord: any = {};
        finalHeaders.forEach(header => {
            const value = record[header];
             if (typeof value === 'number') {
                newRecord[header] = parseFloat(value.toFixed(3));
            } else {
                newRecord[header] = value;
            }
        });
        delete newRecord['_internalId'];
        delete newRecord['Max_PairScore'];
        return newRecord;
    });
    
    const finalColumns = ws.columns.filter(c => c.key !== '_internalId' && c.key !== 'Max_PairScore');
    ws.columns = finalColumns;

    ws.addRows(dataForSheet);
    
    let lastClusterId: number | string | null = null;
    ws.eachRow({ includeEmpty: false }, (row, rowNumber) => {
        if (rowNumber === 1) return;
        
        const rowData = data[rowNumber - 2]; 
        if (!rowData) return;

        const score = rowData.Max_PairScore ?? -1;
        if (score > 0) {
            let fillColor: string | undefined;
            if (score >= 0.9) { fillColor = 'FFFF0000'; } 
            else if (score >= 0.8) { fillColor = 'FFFFC7CE'; } 
            else if (score >= 0.7) { fillColor = 'FFFFC000'; } 
            else { fillColor = 'FFFFFF00'; }

            if (fillColor) {
                const fill = { type: 'pattern' as const, pattern: 'solid' as const, fgColor: { argb: fillColor } };
                 row.eachCell({ includeEmpty: false }, (cell) => {
                    if (Number(cell.address.replace(/[A-Z]/g, '')) === rowNumber) {
                        cell.fill = fill;
                         if (score >= 0.9) {
                            cell.font = { ...cell.font, bold: true, color: { argb: 'FFFFFFFF' } };
                        }
                    }
                });
            }
        }
        
        const cid = rowData.Generated_Cluster_ID;
        if (cid !== null && cid !== lastClusterId && lastClusterId !== null) {
            row.eachCell({ includeEmpty: false }, (cell) => {
               if (Number(cell.address.replace(/[A-Z]/g, '')) === rowNumber) {
                    cell.border = { ...cell.border, top: { style: 'thick', color: { argb: 'FF002060' } } };
               }
            });
        }
        lastClusterId = cid;
    });
}

function createSummarySheet(wb: ExcelJS.Workbook, allRecords: RecordRow[], clusters: {records: RecordRow[], confidenceScore?: number}[], auditFindings: AuditFinding[]) {
    const ws = wb.addWorksheet("Review Summary");
    ws.views = [{ rightToLeft: true }];
    
    ws.columns = [ { width: 5 }, { width: 25 }, { width: 5 }, { width: 25 }, { width: 5 }];

    ws.mergeCells('B2:D2');
    const titleCell = ws.getCell('B2');
    titleCell.value = "تقرير مراجعة المجموعات";
    titleCell.font = { size: 24, bold: true, name: 'Calibri' };
    titleCell.alignment = { horizontal: 'center', vertical: 'middle' };
    ws.getRow(2).height = 40;

    const totalRecords = allRecords.length;
    const clusteredRecordsCount = new Set(clusters.flatMap(c => c.records.map(r => r._internalId))).size;
    const numClusters = clusters.length;
    const unclusteredCount = totalRecords - clusteredRecordsCount;
    const avgClusterSize = numClusters > 0 ? (clusteredRecordsCount / numClusters) : 0;
    const clusteredPercentage = totalRecords > 0 ? (clusteredRecordsCount / totalRecords) : 0;

    const summaryStats = [
        [{ title: "إجمالي السجلات المعالجة", value: totalRecords, icon: "👥" }, { title: "عدد المجموعات", value: numClusters, icon: "📁" }],
        [{ title: "السجلات المجمعة", value: clusteredRecordsCount, icon: "🔗" }, { title: "السجلات غير المجمعة", value: unclusteredCount, icon: "👤" }],
        [{ title: "متوسط حجم المجموعة", value: avgClusterSize.toFixed(2), icon: "📊" }, { title: "نسبة السجلات المجمعة", value: `${(clusteredPercentage * 100).toFixed(1)}%`, icon: "📈" }]
    ];
    
    let summaryCurrentRow = 4;
    summaryStats.forEach(rowItems => {
        ws.getRow(summaryCurrentRow).height = 45;
        rowItems.forEach((stat, colIndex) => {
            const startColNum = colIndex === 0 ? 2 : 4;
            ws.mergeCells(summaryCurrentRow, startColNum, summaryCurrentRow + 3, startColNum);
            const cardCell = ws.getCell(summaryCurrentRow, startColNum);
            cardCell.value = { richText: [ { text: `${stat.icon}`, font: { size: 36, name: 'Segoe UI Emoji' } }, { text: `\n${stat.title}\n`, font: { size: 14 } }, { text: `${stat.value}`, font: { size: 24, bold: true } } ] };
            cardCell.alignment = { vertical: 'middle', horizontal: 'center', wrapText: true };
            cardCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFE6F2FF' } };
            cardCell.border = { top: { style: 'thin' }, left: { style: 'thin' }, bottom: { style: 'thin' }, right: { style: 'thin' } };
        });
        summaryCurrentRow += 5;
    });


    // --- Decision Counts ---
    let decisionCurrentRow = summaryCurrentRow + 1;
    ws.getRow(decisionCurrentRow).height = 15;

    const decisionCounts = { 'تكرار مؤكد': 0, 'اشتباه تكرار مؤكد': 0, 'اشتباه تكرار': 0, 'إحتمالية تكرار': 0 };

    clusters.forEach(clusterObj => {
        const { decision } = getDecisionAndNote(clusterObj.confidenceScore || 0);
        if (decision in decisionCounts) {
            decisionCounts[decision as keyof typeof decisionCounts]++;
        }
    });
    
    const decisionStats = [
        [{ title: "تكرار مؤكد", value: decisionCounts['تكرار مؤكد'], icon: "🚨" }, { title: "اشتباه تكرار مؤكد", value: decisionCounts['اشتباه تكرار مؤكد'], icon: "⚠️" }],
        [{ title: "اشتباه تكرار", value: decisionCounts['اشتباه تكرار'], icon: "🔍" }, { title: "إحتمالية تكرار", value: decisionCounts['إحتمالية تكرار'], icon: "💡" }],
    ];

    decisionCurrentRow++;
    decisionStats.forEach(rowItems => {
        ws.getRow(decisionCurrentRow).height = 45;
        rowItems.forEach((stat, colIndex) => {
            if (!stat) return;
            const startColNum = colIndex === 0 ? 2 : 4;
            ws.mergeCells(decisionCurrentRow, startColNum, decisionCurrentRow + 3, startColNum);
            const cardCell = ws.getCell(decisionCurrentRow, startColNum);
            cardCell.value = { richText: [ { text: `${stat.icon}`, font: { size: 36, name: 'Segoe UI Emoji' } }, { text: `\n${stat.title}\n`, font: { size: 14 } }, { text: `${stat.value}`, font: { size: 24, bold: true } } ] };
            cardCell.alignment = { vertical: 'middle', horizontal: 'center', wrapText: true };
            cardCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF2F2F2' } };
            cardCell.border = { top: { style: 'thin' }, left: { style: 'thin' }, bottom: { style: 'thin' }, right: { style: 'thin' } };
        });
        decisionCurrentRow += 5;
    });

    let auditCurrentRow = decisionCurrentRow + 1;
    ws.getRow(auditCurrentRow - 1).height = 15;

    // --- Audit Summary Data ---
    if (auditFindings && auditFindings.length > 0) {
        ws.mergeCells(`B${auditCurrentRow}:D${auditCurrentRow}`);
        const auditTitleCell = ws.getCell(`B${auditCurrentRow}`);
        auditTitleCell.value = "ملخص نتائج التدقيق";
        auditTitleCell.font = { size: 18, bold: true, name: 'Calibri' };
        auditTitleCell.alignment = { horizontal: 'center', vertical: 'middle' };
        ws.getRow(auditCurrentRow).height = 30;
        auditCurrentRow++;
        
        const findingCounts: Record<string, number> = {
          TOTAL_UNIQUE_RECORDS: new Set(auditFindings.flatMap(f => f.records.map(r => r._internalId))).size,
          WOMAN_MULTIPLE_HUSBANDS: 0, MULTIPLE_NATIONAL_IDS: 0, DUPLICATE_ID: 0, DUPLICATE_COUPLE: 0, HIGH_SIMILARITY: 0
        };

        auditFindings.forEach(f => {
            if (f.type in findingCounts) {
                 findingCounts[f.type] += new Set(f.records.map(r => r._internalId)).size;
            }
        });

        const auditSummaryCards = [
            [{ title: "السجلات المدققة الفريدة", key: 'TOTAL_UNIQUE_RECORDS', icon: '🛡️' }, { title: "ازدواجية الزوجين", key: 'DUPLICATE_COUPLE', icon: '👨‍👩‍👧‍👦' }],
            [{ title: "تعدد الأزواج", key: 'WOMAN_MULTIPLE_HUSBANDS', icon: '🙍‍♀️' }, { title: "تعدد أرقام الهوية", key: 'MULTIPLE_NATIONAL_IDS', icon: '💳' }],
            [{ title: "ازدواجية الرقم القومي", key: 'DUPLICATE_ID', icon: '🧾' }, { title: "تشابه عالي", key: 'HIGH_SIMILARITY', icon: '✨' }]
        ];
        
        auditSummaryCards.forEach((rowItems) => {
            ws.getRow(auditCurrentRow).height = 45;
            rowItems.forEach((stat, colIndex) => {
                if (!stat) return;
                const startColNum = colIndex === 0 ? 2 : 4;
                ws.mergeCells(auditCurrentRow, startColNum, auditCurrentRow + 3, startColNum);
                const cardCell = ws.getCell(auditCurrentRow, startColNum);
                const count = findingCounts[stat.key];
                cardCell.value = { richText: [ { text: `${stat.icon}`, font: { size: 36, name: 'Segoe UI Emoji' } }, { text: `\n${stat.title}\n`, font: { size: 14 } }, { text: `${count}`, font: { size: 24, bold: true } } ] };
                cardCell.alignment = { vertical: 'middle', horizontal: 'center', wrapText: true };
                cardCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF2F2F2' } };
                cardCell.border = { top: { style: 'thin' }, left: { style: 'thin' }, bottom: { style: 'thin' }, right: { style: 'thin' } };
            });
            auditCurrentRow += 5;
        });
    }
}


function createClustersSheet(wb: ExcelJS.Workbook, clusters: any[]) {
    const ws = wb.addWorksheet("Cluster Details");
    ws.views = [{ rightToLeft: true }];

    const headers = ["Cluster ID", "AI Summary", "Beneficiary ID", "Score", "Woman Name", "Husband Name", "National ID", "Phone", "Children"];
    ws.columns = headers.map(h => ({ 
        header: h, 
        key: h.replace(/\s/g, ''), 
        width: h === 'AI Summary' ? 50 : (h === 'Woman Name' || h === 'Husband Name' ? 25 : 15)
    }));
    
    const headerRow = ws.getRow(1);
    headerRow.font = { bold: true, color: { argb: 'FFFFFFFF' } };
    headerRow.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF4F81BD' } };
    headerRow.alignment = { horizontal: 'center' };

    let currentRowIndex = 2;
    clusters.forEach((clusterObj: any, index) => {
        const clusterRecords = clusterObj.records;
        const clusterId = index + 1;
        if (!clusterRecords || clusterRecords.length < 2) return;
        
        const summaryText = generateArabicClusterSummary(clusterObj, clusterRecords)
            .replace(/<br\s*\/?>/gi, '\n')
            .replace(/<[^>]*>?/gm, '')
            .trim();

        const recordsForSheet = [...clusterRecords].sort((a,b) => String(a.beneficiaryId || '').localeCompare(String(b.beneficiaryId || '')));

        const startRow = currentRowIndex;
        const endRow = startRow + recordsForSheet.length - 1;

        let rowHeight = 40; // Default
        const clusterSize = recordsForSheet.length;
        if (clusterSize === 2) rowHeight = 142;
        if (clusterSize === 3) rowHeight = 95;
        if (clusterSize === 4) rowHeight = 76;
        
        const recordScores = new Map<string, number>();
        const pairs = fullPairwiseBreakdown(clusterRecords);
        clusterRecords.forEach((record: RecordRow) => {
            const relatedPairs = pairs.filter(p => p.a._internalId === record._internalId || p.b._internalId === record._internalId);
            const avgScore = relatedPairs.length > 0 ? relatedPairs.reduce((sum, p) => sum + p.score, 0) / relatedPairs.length : 0;
            recordScores.set(record._internalId!, avgScore);
        });


        recordsForSheet.forEach((record, recordIndex) => {
             const childrenText = Array.isArray(record.children) ? record.children.join(', ') : record.children || '';
             const avgScore = recordScores.get(record._internalId!) || 0;
             
             let rowData: any = {
                BeneficiaryID: record.beneficiaryId,
                Score: parseFloat(avgScore.toFixed(3)),
                WomanName: record.womanName,
                HusbandName: record.husbandName,
                NationalID: record.nationalId,
                Phone: record.phone,
                Children: childrenText
            };
            
            if (recordIndex === 0) {
                rowData['AISummary'] = summaryText;
            }

            ws.addRow(rowData);
        });
        
        ws.mergeCells(`A${startRow}:A${endRow}`);
        const clusterIdCell = ws.getCell(`A${startRow}`);
        clusterIdCell.value = clusterId;
        clusterIdCell.alignment = { vertical: 'top', horizontal: 'center' };
        
        ws.mergeCells(`B${startRow}:B${endRow}`);
        const summaryCell = ws.getCell(`B${startRow}`);
        summaryCell.alignment = { vertical: 'top', horizontal: 'right', wrapText: true };

        for (let i = startRow; i <= endRow; i++) {
            const row = ws.getRow(i);
            row.height = rowHeight;
            row.eachCell({ includeEmpty: true }, (cell, colNumber) => {
                const border: Partial<ExcelJS.Borders> = {};
                if (i === startRow) border.top = { style: 'thick', color: {argb: 'FF4F81BD'} };
                if (i === endRow) border.bottom = { style: 'thick', color: {argb: 'FF4F81BD'} };
                
                cell.border = { ...cell.border, ...border };

                const key = ((ws.columns[colNumber - 1].key || '').replace(/\s/g, ''));
                if (['ClusterID', 'BeneficiaryID', 'Score', 'NationalID', 'Phone', 'Children'].includes(key)) {
                    cell.alignment = { ...cell.alignment, vertical: 'middle', horizontal: 'center', wrapText: true };
                } else if (['WomanName', 'HusbandName'].includes(key)) {
                    cell.alignment = { ...cell.alignment, vertical: 'middle', horizontal: 'right', wrapText: true };
                }
            });
        }
        
        currentRowIndex = endRow + 1;
    });
}
    
function createAuditSheet(wb: ExcelJS.Workbook, findings: AuditFinding[], clusters: {records: RecordRow[]}[]) {
    const ws = wb.addWorksheet("Audit Findings");
    ws.views = [{ rightToLeft: true }];
    
    const recordToClusterIdMap = new Map<string, number>();
    clusters.forEach((clusterObj, index) => {
        clusterObj.records.forEach(record => {
            recordToClusterIdMap.set(record._internalId!, index + 1);
        });
    });

    const headers = [
      { header: "الخطورة", key: "severity", width: 12 },
      { header: "نوع النتيجة", key: "type", width: 40 },
      { header: "الوصف", key: "description", width: 50 },
      { header: "معرف المجموعة", key: "clusterId", width: 15 },
      { header: "معرف المستفيد", key: "beneficiaryId", width: 20 },
      { header: "اسم الزوجة", key: "womanName", width: 25 },
      { header: "اسم الزوج", key: "husbandName", width: 25 },
      { header: "الرقم القومي", key: "nationalId", width: 20 },
      { header: "الهاتف", key: "phone", width: 20 },
    ];
    ws.columns = headers;
    
    const headerRow = ws.getRow(1);
    headerRow.eachCell((cell) => {
      cell.font = { bold: true, color: { argb: 'FFFFFFFF' } };
      cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FFC00000" } };
      cell.alignment = { horizontal: "center" };
    });

    const severityOrder = { high: 1, medium: 2, low: 3 };
    const severityTranslations: Record<string, string> = { high: 'عالية', medium: 'متوسطة', low: 'منخفضة' };
    const typeTranslations: Record<string, string> = {
        "DUPLICATE_ID": "تكرار الرقم القومي",
        "DUPLICATE_COUPLE": "ازدواجية الزوجين",
        "WOMAN_MULTIPLE_HUSBANDS": "زوجة لديها عدة أزواج",
        "HUSBAND_TOO_MANY_WIVES": "زوج لديه أكثر من 4 زوجات",
        "MULTIPLE_NATIONAL_IDS": "زوجة لديها عدة أرقام قومية",
        "HIGH_SIMILARITY": "تشابه عالي بين السجلات"
    };

    const descriptionTranslations: Record<string, (finding: AuditFinding, record: RecordRow) => string> = {
        "DUPLICATE_ID": () => `الرقم القومي مكرر داخل المجموعة.`,
        "DUPLICATE_COUPLE": () => `تطابق تام لاسم الزوجة والزوج.`,
        "WOMAN_MULTIPLE_HUSBANDS": (f) => `الزوجة مسجلة مع عدة أزواج: ${[...new Set(f.records.map(rec => rec.husbandName))].join(', ')}`,
        "HUSBAND_TOO_MANY_WIVES": (f) => `الزوج مسجل مع ${new Set(f.records.map(rec => rec.womanName)).size} زوجات، وهو ما يتجاوز الحد المسموح به.`,
        "MULTIPLE_NATIONAL_IDS": (f, r) => `الزوجة '${r.womanName}' مرتبطة بعدة أرقام قومية: ${[...new Set(f.records.filter(rec => rec.womanName === r.womanName).map(rec=>rec.nationalId))].join(', ')}`,
        "HIGH_SIMILARITY": (f) => `يوجد تشابه عالي في البيانات بين السجلات داخل هذه المجموعة.`,
    };

    const beneficiaryFindings = new Map<string, any>();
    findings.forEach(finding => {
        finding.records.forEach(record => {
            const beneficiaryId = record.beneficiaryId;
            if (!beneficiaryId) return;

            const existing = beneficiaryFindings.get(beneficiaryId);
            const translatedDescription = descriptionTranslations[finding.type] ? descriptionTranslations[finding.type](finding, record) : finding.description;

            if (existing) {
                if (severityOrder[finding.severity as keyof typeof severityOrder] < severityOrder[existing.severityValue as keyof typeof severityOrder]) {
                    existing.severity = finding.severity;
                    existing.severityValue = finding.severity;
                }
                existing.types.add(finding.type);
                existing.descriptions.add(translatedDescription);
            } else {
                beneficiaryFindings.set(beneficiaryId, {
                    ...record,
                    severity: finding.severity,
                    severityValue: finding.severity,
                    types: new Set([finding.type]),
                    descriptions: new Set([translatedDescription]),
                    clusterId: recordToClusterIdMap.get(record._internalId!) || 'N/A'
                });
            }
        });
    });

    let consolidatedData = Array.from(beneficiaryFindings.values()).map(f => ({
        ...f,
        type: Array.from(f.types).join(' + '),
        description: Array.from(f.descriptions).join(' + ')
    }));

    const clusterGroups = new Map<string, any[]>();
    consolidatedData.forEach(record => {
        const clusterId = record.clusterId;
        if (!clusterGroups.has(clusterId)) {
            clusterGroups.set(clusterId, []);
        }
        clusterGroups.get(clusterId)!.push(record);
    });

    let finalData: any[] = [];
    clusterGroups.forEach((records) => {
        if (records.length === 0) return;

        let highestSeverityValue: 'high' | 'medium' | 'low' = 'low';
        const combinedTypes = new Set<string>();
        const combinedDescriptions = new Set<string>();

        records.forEach(record => {
            if (severityOrder[record.severityValue as keyof typeof severityOrder] < severityOrder[highestSeverityValue]) {
                highestSeverityValue = record.severityValue;
            }
            record.type.split(' + ').forEach((t: string) => combinedTypes.add(t));
            record.description.split(' + ').forEach((d: string) => combinedDescriptions.add(d));
        });

        const unifiedType = Array.from(combinedTypes).map(t => typeTranslations[t] || t.replace(/_/g, ' ')).join(' + ');
        const unifiedDescription = Array.from(combinedDescriptions).join(' + ');
        const unifiedSeverity = severityTranslations[highestSeverityValue] || highestSeverityValue;
        
        const unifiedRecords = records.map(record => ({
            ...record,
            severity: unifiedSeverity,
            severityValue: highestSeverityValue,
            type: unifiedType,
            description: unifiedDescription,
        }));
        finalData.push(...unifiedRecords);
    });
    
    finalData.sort((a, b) => {
        const severityComparison = severityOrder[a.severityValue as keyof typeof severityOrder] - severityOrder[b.severityValue as keyof typeof severityOrder];
        if (severityComparison !== 0) return severityComparison;
        
        const clusterIdA = a.clusterId === 'N/A' ? Infinity : a.clusterId;
        const clusterIdB = b.clusterId === 'N/A' ? Infinity : b.clusterId;
        if (clusterIdA !== clusterIdB) return clusterIdA - clusterIdB;

        return String(a.beneficiaryId || '').localeCompare(String(b.beneficiaryId || ''));
    });

    let lastClusterId: string | number | null = null;
    finalData.forEach((data, index) => {
        const row = ws.addRow({
            severity: data.severity, type: data.type, description: data.description, clusterId: data.clusterId, beneficiaryId: data.beneficiaryId,
            womanName: data.womanName, husbandName: data.husbandName, nationalId: data.nationalId, phone: data.phone,
        });

        if (index > 0 && data.clusterId !== lastClusterId) {
           row.border = { ...row.border, top: { style: 'thick', color: { argb: 'FF4F81BD' } } };
        }
        lastClusterId = data.clusterId;

        const severityColor = data.severityValue === 'high' ? 'FFFFC7CE' : data.severityValue === 'medium' ? 'FFFFEB9C' : 'FFC6EFCE';
        row.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: severityColor } };
        row.alignment = { vertical: 'middle', wrapText: true };
    });
}
function createDashboardReportSheet(wb: ExcelJS.Workbook, chartImages: Record<string, string>, processedData: any, worker: any) {
    const ws = wb.addWorksheet("Dashboard Report");
    ws.views = [{ rightToLeft: true }];
    
    ws.columns = [
        { width: 2 },  // A
        { width: 20 }, // B
        { width: 20 }, // C
        { width: 16 },  // D
        { width: 20 }, // E
        { width: 20 }, // F
    ];


    ws.mergeCells('B2:F2');
    const titleCell = ws.getCell('B2');
    titleCell.value = "Analysis Dashboard Report";
    titleCell.font = { name: 'Calibri', size: 24, bold: true, color: { argb: 'FF002060' } };
    titleCell.alignment = { horizontal: 'center' };
    ws.getRow(2).height = 30;

    const kf = processedData.keyFigures;
    const keyFiguresData = [
        { title: 'Team Leaders', value: kf.teamLeaders, cell: 'B4' },
        { title: 'Surveyors', value: kf.surveyors, cell: 'C4' },
        { title: 'Registration Days', value: kf.registrationDays, cell: 'E4' },
        { title: 'Villages Targeted', value: kf.villages, cell: 'F4' },
    ];
    
    keyFiguresData.forEach(item => {
        const titleCell = ws.getCell(item.cell);
        titleCell.value = item.title;
        titleCell.font = { name: 'Calibri', size: 12, bold: true, color: { argb: 'FFFFFFFF' } };
        titleCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF4F81BD' } };
        titleCell.alignment = { horizontal: 'center', vertical: 'middle' };

        const valueCell = ws.getCell(item.cell.replace('4', '5'));
        valueCell.value = item.value;
        valueCell.font = { name: 'Calibri', size: 20, bold: true, color: { argb: 'FF002060' } };
        valueCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFDCE6F1' } };
        valueCell.alignment = { horizontal: 'center', vertical: 'middle' };
    });
    ws.getRow(5).height = 30;


    const addImage = (base64: string, tl: { col: number, row: number }, ext: { width: number, height: number }) => {
        if (!base64 || !base64.startsWith('data:image/png;base64,')) return;

        const imageId = wb.addImage({
            base64: base64.split(',')[1],
            extension: 'png',
        });
        
        ws.addImage(imageId, { tl, ext });
    };
    
    let currentRow = 7;
    const rowGap = 1;

    if (chartImages.byDayChart) {
      addImage(chartImages.byDayChart, { col: 1, row: currentRow }, { width: 347, height: 788 });
    }
    if (chartImages.byVillageChart) {
      addImage(chartImages.byVillageChart, { col: 4, row: currentRow }, { width: 347, height: 788 });
    }
    currentRow += Math.round(788 / 15) + rowGap;

    if (chartImages.womenDonut) {
      addImage(chartImages.womenDonut, { col: 1, row: currentRow }, { width: 347, height: 359 });
    }
    if (chartImages.genderVisual) {
      addImage(chartImages.genderVisual, { col: 4, row: currentRow }, { width: 347, height: 359 });
    }
    currentRow += Math.round(359 / 15) + rowGap;

    if (chartImages.bubbleStats) {
        addImage(chartImages.bubbleStats, { col: 1, row: currentRow }, { width: 347, height: 749 });
    }
    if (chartImages.map) {
        addImage(chartImages.map, { col: 4, row: currentRow }, { width: 347, height: 749 });
    }
}
