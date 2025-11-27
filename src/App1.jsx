


import React, { useState, useEffect, useRef, createContext, useContext, useCallback } from 'react';
import { 
  Camera, Upload, ArrowLeft, Image as ImageIcon, Loader2, 
  Search, X, Heart, Trash2, CheckCircle, AlertCircle, Plus 
} from 'lucide-react';

/**
 * ==========================================
 * 1. UTILS & MOCK DATA
 * ==========================================
 */

// Simulated database with varying aspect ratios for Masonry layout
const INITIAL_DB = [
  { id: '1', title: 'Mountain Peaks', likes: 24, data: 'https://images.unsplash.com/photo-1506905925346-21bda4d32df4?auto=format&fit=crop&w=800&q=80' },
  { id: '2', title: 'Ocean Sunset', likes: 156, data: 'https://images.unsplash.com/photo-1471922694854-ff1b63b20054?auto=format&fit=crop&w=800&q=80' },
  { id: '3', title: 'Urban Architecture', likes: 45, data: 'https://images.unsplash.com/photo-1449824913929-2b5a616cf6c2?auto=format&fit=crop&w=800&q=80' },
  { id: '4', title: 'Misty Forest', likes: 89, data: 'https://images.unsplash.com/photo-1441974231531-c6227db76b6e?auto=format&fit=crop&w=800&q=80' },
  { id: '5', title: 'Desert Dunes', likes: 12, data: 'https://images.unsplash.com/photo-1509316975850-ff9c5deb0cd9?auto=format&fit=crop&w=800&q=80' },
  { id: '6', title: 'Winter Wilderness', likes: 67, data: 'https://images.unsplash.com/photo-1483664852095-d6cc6870705d?auto=format&fit=crop&w=800&q=80' },
  { id: '7', title: 'Abstract Shapes', likes: 33, data: 'https://images.unsplash.com/photo-1550684848-fac1c5b4e853?auto=format&fit=crop&w=800&q=80' },
  { id: '8', title: 'Neon Nights', likes: 210, data: 'https://images.unsplash.com/photo-1555685812-4b943f1cb0eb?auto=format&fit=crop&w=800&q=80' },
];

/**
 * MOCK API SERVICE
 */
const MockApi = {
  db: [...INITIAL_DB],

  getImages: async () => {
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve([...MockApi.db]);
      }, 800);
    });
  },

  uploadPhoto: async (fileObj, onProgress) => {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.readAsDataURL(fileObj);
      
      // Simulate progress
      let progress = 0;
      const interval = setInterval(() => {
        progress += 10;
        if (onProgress) onProgress(progress);
        if (progress >= 100) clearInterval(interval);
      }, 100);

      reader.onload = () => {
        setTimeout(() => {
          clearInterval(interval);
          const newImage = {
            id: Date.now().toString(),
            title: fileObj.name.split('.')[0] || 'Untitled',
            likes: 0,
            data: reader.result
          };
          MockApi.db = [newImage, ...MockApi.db];
          resolve(newImage);
        }, 1200);
      };
      
      reader.onerror = () => {
        clearInterval(interval);
        reject(new Error("Failed to read file"));
      };
    });
  },

  deletePhoto: async (id) => {
    return new Promise((resolve) => {
      setTimeout(() => {
        MockApi.db = MockApi.db.filter(img => img.id !== id);
        resolve(true);
      }, 500);
    });
  },

  toggleLike: async (id) => {
    return new Promise((resolve) => {
      setTimeout(() => {
        const index = MockApi.db.findIndex(img => img.id === id);
        if (index > -1) {
          // Toggle logic simulation
          // In a real app, this would be handled by the backend
          MockApi.db[index].likes += 1; 
          resolve(MockApi.db[index]);
        }
      }, 200);
    });
  },

  getSimilarImages: async (photoId) => {
    return new Promise((resolve) => {
      setTimeout(() => {
        const filtered = MockApi.db.filter(img => img.id !== photoId);
        // Deterministic shuffle for demo consistency
        const shuffled = filtered.sort(() => 0.5 - Math.random());
        resolve(shuffled.slice(0, 4));
      }, 1000);
    });
  }
};

/**
 * ==========================================
 * 2. CONTEXTS & HOOKS
 * ==========================================
 */

const ToastContext = createContext(null);

const ToastProvider = ({ children }) => {
  const [toasts, setToasts] = useState([]);

  const addToast = useCallback((message, type = 'success') => {
    const id = Date.now();
    setToasts(prev => [...prev, { id, message, type }]);
    setTimeout(() => {
      setToasts(prev => prev.filter(t => t.id !== id));
    }, 3000);
  }, []);

  const removeToast = (id) => {
    setToasts(prev => prev.filter(t => t.id !== id));
  };

  return (
    <ToastContext.Provider value={{ addToast }}>
      {children}
      <div className="fixed bottom-4 right-4 z-[60] flex flex-col gap-2 pointer-events-none">
        {toasts.map(toast => (
          <div 
            key={toast.id}
            className={`
              pointer-events-auto flex items-center gap-2 px-4 py-3 rounded-lg shadow-lg text-white transform transition-all animate-in slide-in-from-right fade-in
              ${toast.type === 'success' ? 'bg-emerald-600' : 'bg-red-500'}
            `}
          >
            {toast.type === 'success' ? <CheckCircle size={18} /> : <AlertCircle size={18} />}
            <span className="text-sm font-medium">{toast.message}</span>
            <button onClick={() => removeToast(toast.id)} className="ml-2 hover:opacity-80">
              <X size={14} />
            </button>
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  );
};

const useToast = () => useContext(ToastContext);

/**
 * ==========================================
 * 3. UI COMPONENTS
 * ==========================================
 */

const Button = ({ children, onClick, variant = 'primary', icon: Icon, disabled, className = '' }) => {
  const baseStyles = "flex items-center justify-center gap-2 px-4 py-2 rounded-lg font-medium transition-all duration-200 active:scale-95 disabled:opacity-50 disabled:cursor-not-allowed";
  
  const variants = {
    primary: "bg-indigo-600 text-white hover:bg-indigo-700 shadow-md hover:shadow-indigo-200",
    secondary: "bg-white text-slate-700 border border-slate-200 hover:bg-slate-50 hover:border-slate-300 shadow-sm",
    danger: "bg-rose-50 text-rose-600 hover:bg-rose-100 border border-rose-100",
    ghost: "text-slate-500 hover:bg-slate-100"
  };

  return (
    <button 
      onClick={onClick} 
      disabled={disabled}
      className={`${baseStyles} ${variants[variant]} ${className}`}
    >
      {Icon && <Icon size={18} />}
      {children}
    </button>
  );
};

const Modal = ({ isOpen, onClose, title, children }) => {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm transition-opacity" onClick={onClose} />
      <div className="relative bg-white rounded-2xl w-full max-w-lg shadow-2xl scale-100 transition-transform duration-300 animate-in zoom-in-95">
        <div className="flex items-center justify-between p-6 border-b border-slate-100">
          <h3 className="text-lg font-bold text-slate-800">{title}</h3>
          <button onClick={onClose} className="p-2 hover:bg-slate-100 rounded-full text-slate-400 hover:text-slate-600 transition-colors">
            <X size={20} />
          </button>
        </div>
        <div className="p-6">
          {children}
        </div>
      </div>
    </div>
  );
};

/**
 * ==========================================
 * 4. FEATURE COMPONENTS
 * ==========================================
 */

const UploadDropzone = ({ onUploadSuccess, onClose }) => {
  const [isDragging, setIsDragging] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [isUploading, setIsUploading] = useState(false);
  const fileInputRef = useRef(null);
  const { addToast } = useToast();

  const handleFile = async (file) => {
    if (!file || !file.type.startsWith('image/')) {
      addToast("Please select a valid image file", "error");
      return;
    }

    setIsUploading(true);
    try {
      await MockApi.uploadPhoto(file, (progress) => setUploadProgress(progress));
      addToast("Photo uploaded successfully!");
      onUploadSuccess(); // Trigger refresh in parent
      onClose(); // Close modal
    } catch (error) {
      addToast("Upload failed. Please try again.", "error");
    } finally {
      setIsUploading(false);
      setUploadProgress(0);
    }
  };

  const handleDrop = (e) => {
    e.preventDefault();
    setIsDragging(false);
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      handleFile(e.dataTransfer.files[0]);
    }
  };

  return (
    <div className="w-full">
      {isUploading ? (
        <div className="py-12 flex flex-col items-center justify-center text-center">
          <Loader2 size={48} className="text-indigo-600 animate-spin mb-4" />
          <h4 className="text-lg font-semibold text-slate-800 mb-2">Uploading Photo...</h4>
          <div className="w-64 h-2 bg-slate-100 rounded-full overflow-hidden mt-2">
            <div 
              className="h-full bg-indigo-600 transition-all duration-300 ease-out"
              style={{ width: `${uploadProgress}%` }}
            />
          </div>
          <p className="text-xs text-slate-400 mt-2">{uploadProgress}% Complete</p>
        </div>
      ) : (
        <div
          onDragOver={(e) => { e.preventDefault(); setIsDragging(true); }}
          onDragLeave={() => setIsDragging(false)}
          onDrop={handleDrop}
          onClick={() => fileInputRef.current?.click()}
          className={`
            border-2 border-dashed rounded-xl p-10 text-center cursor-pointer transition-all duration-300
            ${isDragging ? 'border-indigo-500 bg-indigo-50' : 'border-slate-200 hover:border-indigo-300 hover:bg-slate-50'}
          `}
        >
          <input 
            type="file" 
            ref={fileInputRef} 
            className="hidden" 
            accept="image/*"
            onChange={(e) => handleFile(e.target.files[0])}
          />
          <div className="w-16 h-16 bg-indigo-100 text-indigo-600 rounded-full flex items-center justify-center mx-auto mb-4">
            <Upload size={32} />
          </div>
          <h4 className="text-lg font-semibold text-slate-800">Click or drag image to upload</h4>
          <p className="text-slate-500 text-sm mt-2">Supports JPG, PNG, GIF up to 10MB</p>
        </div>
      )}
    </div>
  );
};

const ImageCard = ({ image, onClick, onDelete, onLike }) => {
  const [liked, setLiked] = useState(false); // Local optimistic state

  const handleLike = (e) => {
    e.stopPropagation();
    setLiked(!liked);
    onLike(image.id);
  };

  const handleDelete = (e) => {
    e.stopPropagation();
    onDelete(image.id);
  };

  return (
    <div 
      className="group relative mb-6 break-inside-avoid rounded-xl overflow-hidden cursor-pointer shadow-sm hover:shadow-xl transition-all duration-500 bg-slate-900"
      onClick={() => onClick(image)}
    >
      <img 
        src={image.data} 
        alt={image.title} 
        className="w-full h-auto object-cover transform transition-transform duration-700 group-hover:scale-105 opacity-90 group-hover:opacity-100"
      />
      
      {/* Overlay controls */}
      <div className="absolute inset-0 bg-gradient-to-t from-black/90 via-black/20 to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-300 flex flex-col justify-end p-4">
        <div className="flex items-center justify-between mb-2">
          <h3 className="text-white font-medium truncate flex-1 pr-2">{image.title}</h3>
          <button 
            onClick={handleDelete}
            className="p-2 bg-white/10 hover:bg-red-500/80 backdrop-blur-sm rounded-full text-white transition-colors"
            title="Delete Image"
          >
            <Trash2 size={16} />
          </button>
        </div>
        
        <div className="flex items-center justify-between">
          <button 
            onClick={handleLike}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-semibold backdrop-blur-md transition-all ${liked ? 'bg-rose-500 text-white' : 'bg-white/20 text-white hover:bg-white/30'}`}
          >
            <Heart size={12} className={liked ? 'fill-current' : ''} />
            {image.likes + (liked ? 1 : 0)}
          </button>
          <span className="text-xs text-blue-200 font-medium flex items-center gap-1">
            <Search size={12} /> Find Similar
          </span>
        </div>
      </div>
    </div>
  );
};

/**
 * ==========================================
 * 5. MAIN APP COMPONENT
 * ==========================================
 */

const GalleryApp = () => {
  // Global State
  const [view, setView] = useState('home');
  const [images, setImages] = useState([]);
  const [loading, setLoading] = useState(true);
  const [isUploadModalOpen, setIsUploadModalOpen] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  
  // Specific View State
  const [selectedImage, setSelectedImage] = useState(null);
  const [similarImages, setSimilarImages] = useState([]);
  
  const { addToast } = useToast();

  // Load Initial Data
  useEffect(() => {
    fetchImages();
  }, []);

  const fetchImages = async () => {
    try {
      const data = await MockApi.getImages();
      setImages(data);
    } catch (e) {
      addToast("Failed to load gallery", "error");
    } finally {
      setLoading(false);
    }
  };

  // Handlers
  const handleDelete = async (id) => {
    if (window.confirm("Are you sure you want to delete this photo?")) {
      await MockApi.deletePhoto(id);
      setImages(prev => prev.filter(img => img.id !== id));
      addToast("Photo deleted", "success");
    }
  };

  const handleLike = async (id) => {
    await MockApi.toggleLike(id);
    // In a real app we would update the state with the return value
    // Here we rely on optimistic UI in the card mostly, but let's update global state too
    setImages(prev => prev.map(img => 
      img.id === id ? { ...img, likes: img.likes + 1 } : img
    ));
  };

  const handleFindSimilar = async (image) => {
    setSelectedImage(image);
    setView('similar');
    setSimilarImages([]); // clear old
    window.scrollTo(0,0);
    
    try {
      const results = await MockApi.getSimilarImages(image.id);
      setSimilarImages(results);
    } catch (e) {
      addToast("Could not find similar images", "error");
    }
  };

  const handleGoHome = () => {
    setView('home');
    setSelectedImage(null);
    setSearchTerm('');
  };

  // Filter Logic
  const filteredImages = images.filter(img => 
    img.title.toLowerCase().includes(searchTerm.toLowerCase())
  );

  return (
    <div className="min-h-screen bg-slate-50 font-sans text-slate-900 selection:bg-indigo-100 selection:text-indigo-900">
      
      {/* Navigation Bar */}
      <nav className="sticky top-0 z-40 bg-white/80 backdrop-blur-xl border-b border-slate-200/60 shadow-sm transition-all">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-20 flex items-center justify-between gap-4">
          
          <div className="flex items-center gap-3 min-w-fit">
            {view === 'similar' ? (
              <Button onClick={handleGoHome} variant="ghost" className="!px-2 !-ml-2">
                <ArrowLeft size={24} />
              </Button>
            ) : (
              <div className="w-10 h-10 bg-gradient-to-br from-indigo-600 to-violet-600 rounded-xl flex items-center justify-center text-white shadow-indigo-200 shadow-lg">
                <Camera size={20} />
              </div>
            )}
            <div>
              <h1 className="text-xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-indigo-700 to-violet-700 tracking-tight">
                {view === 'home' ? 'PhotoStream' : 'SmartMatch'}
              </h1>
              {view === 'similar' && <p className="text-xs text-slate-400 font-medium">AI-Powered Similarity Search</p>}
            </div>
          </div>

          {/* Search Bar (Only visible on Home) */}
          {view === 'home' && (
            <div className="hidden md:flex flex-1 max-w-md mx-4 relative group">
              <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none text-slate-400 group-focus-within:text-indigo-500 transition-colors">
                <Search size={18} />
              </div>
              <input
                type="text"
                placeholder="Search your memories..."
                className="block w-full pl-10 pr-3 py-2.5 bg-slate-100 border-none rounded-full text-sm focus:ring-2 focus:ring-indigo-500 focus:bg-white transition-all shadow-inner"
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
              />
            </div>
          )}

          {/* Action Area */}
          <div className="flex items-center gap-2">
            {view === 'home' && (
              <Button 
                onClick={() => setIsUploadModalOpen(true)} 
                variant="primary" 
                icon={Plus}
                className="hidden sm:flex"
              >
                Upload
              </Button>
            )}
            
            {/* Mobile Fab alternative (could be added for smaller screens) */}
            <button 
              onClick={() => setIsUploadModalOpen(true)}
              className="sm:hidden p-3 bg-indigo-600 text-white rounded-full shadow-lg"
            >
              <Upload size={20} />
            </button>
          </div>
        </div>
      </nav>

      {/* Main Content Area */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        
        {/* --- VIEW: HOME --- */}
        {view === 'home' && (
          <div className="animate-in fade-in slide-in-from-bottom-4 duration-500">
            
            {/* Stats / Header */}
            {!loading && (
              <div className="flex items-baseline justify-between mb-8">
                <h2 className="text-3xl font-bold text-slate-800 tracking-tight">Gallery</h2>
                <span className="text-sm font-medium text-slate-500 bg-slate-100 px-3 py-1 rounded-full">
                  {filteredImages.length} Photos
                </span>
              </div>
            )}

            {/* Content */}
            {loading ? (
              <div className="flex flex-col items-center justify-center h-64">
                <Loader2 size={40} className="text-indigo-600 animate-spin mb-4" />
                <p className="text-slate-400 animate-pulse">Curating your stream...</p>
              </div>
            ) : filteredImages.length === 0 ? (
              <div className="text-center py-20 bg-white rounded-3xl border border-dashed border-slate-300">
                <div className="w-20 h-20 bg-slate-50 rounded-full flex items-center justify-center mx-auto mb-4 text-slate-300">
                  <ImageIcon size={40} />
                </div>
                <h3 className="text-lg font-medium text-slate-900">No photos found</h3>
                <p className="text-slate-500 mb-6">Try searching for something else or upload a new one.</p>
                <Button variant="secondary" onClick={() => setSearchTerm('')}>Clear Search</Button>
              </div>
            ) : (
              /* Masonry Layout via CSS Columns */
              <div className="columns-1 sm:columns-2 lg:columns-3 gap-6 space-y-6">
                {filteredImages.map((img) => (
                  <ImageCard 
                    key={img.id} 
                    image={img} 
                    onClick={handleFindSimilar}
                    onDelete={handleDelete}
                    onLike={handleLike}
                  />
                ))}
              </div>
            )}
          </div>
        )}

        {/* --- VIEW: SIMILAR --- */}
        {view === 'similar' && selectedImage && (
          <div className="animate-in slide-in-from-right duration-500">
            
            {/* Split Layout */}
            <div className="flex flex-col lg:flex-row gap-8 lg:gap-12">
              
              {/* Left: Source Image */}
              <div className="lg:w-1/3">
                <div className="sticky top-24">
                  <div className="rounded-2xl overflow-hidden shadow-2xl shadow-indigo-100 ring-4 ring-white">
                    <img src={selectedImage.data} alt="Source" className="w-full h-auto object-cover" />
                  </div>
                  
                  <div className="mt-6 bg-white p-6 rounded-2xl shadow-sm border border-slate-100">
                    <h2 className="text-2xl font-bold text-slate-800 mb-2">{selectedImage.title}</h2>
                    <div className="flex items-center gap-4 text-sm text-slate-500 mb-6">
                      <span className="flex items-center gap-1"><Heart size={14} className="text-rose-500" /> {selectedImage.likes} Likes</span>
                      <span>•</span>
                      <span>ID: {selectedImage.id}</span>
                    </div>
                    
                    <div className="space-y-3">
                      <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider">Analysis</h4>
                      <div className="flex flex-wrap gap-2">
                         {['Outdoor', 'High Contrast', 'Nature', 'Daylight'].map(tag => (
                           <span key={tag} className="px-3 py-1 bg-slate-100 text-slate-600 rounded-lg text-xs font-medium border border-slate-200">{tag}</span>
                         ))}
                      </div>
                    </div>
                  </div>
                </div>
              </div>

              {/* Right: Results */}
              <div className="lg:w-2/3">
                <div className="mb-6 pb-6 border-b border-slate-200">
                  <h3 className="text-xl font-bold text-slate-800 flex items-center gap-2">
                    Visually Similar
                    {similarImages.length === 0 && <Loader2 size={18} className="animate-spin text-indigo-600" />}
                  </h3>
                  <p className="text-slate-500 mt-1">Based on color palette, composition, and subject matter.</p>
                </div>

                <div className="grid grid-cols-1 sm:grid-cols-2 gap-6">
                  {similarImages.map((img) => (
                    <ImageCard 
                      key={img.id} 
                      image={img} 
                      onClick={handleFindSimilar} // Allow deep diving
                      onDelete={() => {}} // Disable delete in similar view
                      onLike={handleLike}
                    />
                  ))}
                  
                  {similarImages.length === 0 && (
                    <>
                       {[1,2,3,4].map(i => (
                         <div key={i} className="aspect-[4/3] bg-slate-100 rounded-2xl animate-pulse"></div>
                       ))}
                    </>
                  )}
                </div>
              </div>
            </div>
          </div>
        )}

      </main>

      {/* Upload Modal */}
      <Modal 
        isOpen={isUploadModalOpen} 
        onClose={() => setIsUploadModalOpen(false)} 
        title="Upload to Gallery"
      >
        <UploadDropzone 
          onUploadSuccess={fetchImages} 
          onClose={() => setIsUploadModalOpen(false)} 
        />
      </Modal>

    </div>
  );
};

// Wrap with providers
const App = () => (
  <ToastProvider>
    <GalleryApp />
  </ToastProvider>
);

export default App;

