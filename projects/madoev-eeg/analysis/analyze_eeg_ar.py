#!/usr/bin/env python3
"""Exploratory BIC-selected AR(p) on CSV EEG values (microvolts). NOT a clinical tool.

For comparable BIC values, all candidate orders use the SAME target sample segment.
No historic-channel or organ mapping is inferred. Channels must be supplied explicitly.
"""
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd


def fit_order_bic(signal, max_order=20, min_order=1):
    x = np.asarray(signal, dtype=float)
    if x.ndim != 1 or not np.all(np.isfinite(x)):
        raise ValueError('Only finite 1-D signal accepted')
    if len(x) < max(100, 6 * max_order):
        raise ValueError('Not enough observations for candidate AR orders')
    if not (1 <= min_order <= max_order):
        raise ValueError('Invalid range of candidate AR orders')
    # Identical Y for all orders avoids biased comparison caused by variable n.
    x = x - np.mean(x)
    y = x[max_order:]
    n = len(y)
    score = []
    for p in range(min_order, max_order + 1):
        # Predictor columns are x[t-1], ..., x[t-p], plus constant.
        lags = np.column_stack([x[max_order-k:len(x)-k] for k in range(1, p+1)])
        X = np.column_stack([np.ones(n), lags])
        coeff, *_ = np.linalg.lstsq(X, y, rcond=None)
        r = y - X @ coeff
        mse = max(float(np.dot(r, r) / n), np.finfo(float).tiny)
        bic = n * np.log(mse) + (p+1) * np.log(n)
        score.append((bic, p, mse))
    best = min(score)
    return {'order':int(best[1]),'bic':float(best[0]),'residual_mse':float(best[2]),
            'candidate_orders':[int(item[1]) for item in score],
            'bic_curve':[round(float(item[0]),6) for item in score]}


def rolling_estimates(signal, fs, window_sec=20., overlap=.5,
                      max_order=20, ptp_reject_uv=250.):
    if fs <= 0 or window_sec <= 0 or not (0 <= overlap < 1):
        raise ValueError('Bad sampling/window arguments')
    x=np.asarray(signal,dtype=float)
    window=round(fs*window_sec); stride=max(1,round(window*(1-overlap)))
    if window<max(100,max_order*6):
        raise ValueError('Window must contain sufficient samples')
    result=[]; rejected=[]
    for begin in range(0, len(x)-window+1, stride):
        chunk=x[begin:begin+window]
        reason=None
        if not np.all(np.isfinite(chunk)): reason='nonfinite'
        elif np.ptp(chunk)>ptp_reject_uv: reason='peak_to_peak_above_limit'
        elif np.std(chunk)<1e-6: reason='nearly_flat'
        if reason:
            rejected.append({'begin_seconds':begin/fs,'reason':reason})
            continue
        item=fit_order_bic(chunk,max_order=max_order)
        result.append({'begin_seconds':round(begin/fs,4),'end_seconds':round((begin+window)/fs,4),
                       'ar_order':item['order'],'bic':item['bic'],'residual_mse':item['residual_mse']})
    return {'accepted_windows':result, 'rejected_windows':rejected,
            'median_order':float(np.median([r['ar_order'] for r in result])) if result else None,
            'n_accepted':len(result),'n_rejected':len(rejected)}


def assert_atlas_approved(atlas_path):
    atlas=json.loads(Path(atlas_path).read_text(encoding='utf-8'))
    if not atlas.get('clinical_validated') or not atlas.get('usage_policy',{}).get('allow_roi_analysis'):
        raise RuntimeError('Historical atlas NOT approved for organ/ROI mapping: only explicit channel analysis is permitted')
    if not all(reg.get('montage_10_20_channels') for reg in atlas.get('organ_regions',[])):
        raise RuntimeError('Approved atlas cannot contain missing electrode mapping')
    return atlas


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--csv',required=True,help='CSV with numerical EEG channel columns in microvolts')
    p.add_argument('--fs',required=True,type=float,help='Sampling frequency in Hz')
    p.add_argument('--channels',required=True,help='Explicit CSV column names, separated by commas')
    p.add_argument('--out',required=True,help='Output JSON filename')
    p.add_argument('--window',type=float,default=20.)
    p.add_argument('--max-order',type=int,default=20)
    p.add_argument('--max-ptp-uv',type=float,default=250.)
    p.add_argument('--use-organ-atlas',help='Optional validated atlas path; blocks on unverified map')
    args=p.parse_args()
    if args.use_organ_atlas: assert_atlas_approved(args.use_organ_atlas)
    frame=pd.read_csv(args.csv)
    channels=[c.strip() for c in args.channels.split(',') if c.strip()]
    if not channels or len(set(channels))!=len(channels): p.error('Provide distinct, nonempty channel names')
    missing=[c for c in channels if c not in frame.columns]
    if missing: p.error(f'CSV missing channel(s): {missing}')
    result={'source_file':str(Path(args.csv).name),'fs_hz':args.fs,'channels':{},
            'status':'exploratory_signal_analysis_only',
            'limitations':['Not a clinical interpretation','No scalp-to-organ inference',
                           'Requires blinded comparison of within-person baseline windows']}
    for c in channels:
        try:
            signal=pd.to_numeric(frame[c],errors='coerce').to_numpy(dtype=float)
            result['channels'][c]=rolling_estimates(signal,args.fs,args.window,
                max_order=args.max_order,ptp_reject_uv=args.max_ptp_uv)
        except ValueError as e:
            result['channels'][c]={'error':str(e)}
    Path(args.out).write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print('Exploratory AR report:',args.out)

if __name__=='__main__':main()