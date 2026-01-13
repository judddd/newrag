import { useQuery } from '@tanstack/react-query';
import { FileText, HardDrive, PieChart, Activity, Layers } from 'lucide-react';
import { statsAPI } from '../api/stats';

export default function StatsPage() {
  const { data, isLoading } = useQuery({
    queryKey: ['stats'],
    queryFn: statsAPI.get,
    refetchInterval: 5000,
  });

  if (isLoading) {
    return (
      <div className="space-y-8 animate-pulse">
        <div className="h-8 bg-slate-200 dark:bg-slate-800 rounded w-1/4"></div>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {[1, 2, 3].map((i) => (
            <div key={i} className="h-32 bg-slate-200 dark:bg-slate-800 rounded-2xl"></div>
          ))}
        </div>
      </div>
    );
  }

  const stats = [
    {
      label: '总文档数',
      value: data?.total_documents || 0,
      icon: FileText,
      gradient: 'from-blue-500 to-cyan-500',
      shadow: 'shadow-blue-500/20',
    },
    {
      label: '总页数',
      value: (data?.total_pages || 0).toLocaleString(),
      icon: Layers,
      gradient: 'from-violet-500 to-purple-500',
      shadow: 'shadow-violet-500/20',
    },
    {
      label: 'MinIO 存储占用',
      value: `${(data?.total_size_mb || 0).toFixed(2)} MB`,
      icon: HardDrive,
      gradient: 'from-emerald-500 to-teal-500',
      shadow: 'shadow-emerald-500/20',
    },
  ];

  return (
    <div className="space-y-8">
      <div>
        <h2 className="text-2xl font-bold text-slate-900 dark:text-slate-100">
          数据监控大屏
        </h2>
        <p className="text-slate-500 text-sm mt-1">
          实时监控系统知识库状态和资源使用情况
        </p>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {stats.map((stat, index) => (
          <div
            key={index}
            className={`relative overflow-hidden rounded-2xl p-6 text-white shadow-lg ${stat.shadow} hover:-translate-y-1 transition-transform duration-300`}
          >
            <div className={`absolute inset-0 bg-gradient-to-br ${stat.gradient} opacity-90`}></div>
            <div className="absolute inset-0 bg-[url('https://grainy-gradients.vercel.app/noise.svg')] opacity-20"></div>
            
            <div className="relative z-10 flex justify-between items-start">
              <div>
                <p className="text-white/80 text-sm font-medium mb-1">{stat.label}</p>
                <p className="text-4xl font-bold tracking-tight">{stat.value}</p>
              </div>
              <div className="p-3 bg-white/20 backdrop-blur-sm rounded-xl">
                <stat.icon size={24} className="text-white" />
              </div>
            </div>
            
            {/* Decorative circles */}
            <div className="absolute -bottom-6 -right-6 w-24 h-24 bg-white/10 rounded-full blur-xl"></div>
            <div className="absolute top-6 -left-6 w-16 h-16 bg-white/10 rounded-full blur-lg"></div>
          </div>
        ))}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Documents by Type */}
        <div className="card p-6">
          <div className="flex items-center gap-2 mb-6">
            <div className="p-2 bg-blue-50 dark:bg-blue-900/20 rounded-lg text-blue-600">
              <PieChart size={20} />
            </div>
            <h3 className="text-lg font-semibold text-slate-900 dark:text-slate-100">
              文档类型分布
            </h3>
          </div>
          
          <div className="space-y-4">
            {Object.entries(data?.documents_by_type || {}).map(([type, count]) => (
              <div key={type} className="group">
                <div className="flex justify-between items-center mb-1">
                  <span className="text-sm font-medium text-slate-700 dark:text-slate-300 uppercase">{type}</span>
                  <span className="text-sm text-slate-500">{count} 页</span>
                </div>
                <div className="h-2 bg-slate-100 dark:bg-slate-800 rounded-full overflow-hidden">
                  <div 
                    className="h-full bg-blue-500 rounded-full transition-all duration-500"
                    style={{ width: `${(count / (data?.total_documents || 1)) * 100}%` }}
                  ></div>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Documents by Status */}
        <div className="card p-6">
          <div className="flex items-center gap-2 mb-6">
            <div className="p-2 bg-green-50 dark:bg-green-900/20 rounded-lg text-green-600">
              <Activity size={20} />
            </div>
            <h3 className="text-lg font-semibold text-slate-900 dark:text-slate-100">
              处理状态分布
            </h3>
          </div>
          
          <div className="space-y-4">
            {Object.entries(data?.documents_by_status || {}).map(([status, count]) => (
              <div key={status} className="flex items-center justify-between p-3 rounded-xl bg-slate-50 dark:bg-slate-800/50">
                <div className="flex items-center gap-3">
                  <div className={`w-2.5 h-2.5 rounded-full ${
                    status === 'completed' ? 'bg-emerald-500' : 
                    status === 'processing' ? 'bg-amber-500 animate-pulse' : 
                    'bg-rose-500'
                  }`}></div>
                  <span className="text-sm font-medium text-slate-700 dark:text-slate-300 capitalize">
                    {status === 'completed' ? 'Completed' : 
                     status === 'processing' ? 'Processing' : 
                     status === 'failed' ? 'Failed' : status}
                  </span>
                </div>
                <span className="font-mono font-bold text-slate-900 dark:text-slate-100">
                  {count}
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
