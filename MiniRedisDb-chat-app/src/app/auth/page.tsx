import { Button } from "@/components/ui/button";
import Image from "next/image";
import AuthButtons from "./AuthButtons";
import { getKindeServerSession } from "@kinde-oss/kinde-auth-nextjs/server";
import { redirect } from "next/navigation";

const page = async () => {
    const { isAuthenticated } = getKindeServerSession();
    if (await isAuthenticated()) return redirect("/");

    return (
        <div className='flex h-screen w-full'>
            {/* LEFT PANE: The Minimalist Login Card */}
            <div
                // Outer container: takes up screen space with a subtle background
                className='flex-1 flex flex-col p-8 relative justify-center items-center bg-gray-50 dark:bg-gray-900'
            >
                {/* Creative Card Container (Logo + Buttons) with Shadow and Border */}
                <div 
                    className='flex flex-col gap-8 items-center p-12 md:p-16 rounded-3xl bg-white dark:bg-gray-800 max-w-xs sm:max-w-sm w-full transition-all duration-500 
                                
                                // Creative Shadow Effect: Prominent, color-tinted shadow
                                shadow-2xl shadow-red-500/30 dark:shadow-red-900/40 
                                
                                // Subtle Border Accent (Color Theory)
                                border border-red-500/10 dark:border-red-600/20' 
                >
                    
                    {/* MiniRedisDb Logo - Focused and Enhanced */}
                    <Image
                        src={"/logo.png"}
                        alt='MiniRedisDb Logo'
                        width={220} // Slightly increased logo size for focus
                        height={126}
                        // Logo Effect: Soft drop shadow to create a 'glow'
                        className='pointer-events-none select-none filter drop-shadow-lg shadow-red-500/50 dark:shadow-red-400/30' 
                    />

                    {/* Auth Buttons */}
                    <AuthButtons />
                </div>
            </div>
            
            {/* RIGHT PANE: Visual Element (Hero Image) - Less Obtrusive */}
            <div className='flex-1 relative overflow-hidden justify-center items-center hidden md:flex bg-gray-100 dark:bg-gray-800'>
                <div className='absolute inset-0 z-0'>
                    <Image
                        src={"/hero-right.png"}
                        alt='Hero Image'
                        fill
                        // Toned down opacity to make it a background texture/element
                        className='object-cover opacity-50 dark:opacity-20 pointer-events-none select-none'
                    />
                </div>
                
                {/* Optional: Add a subtle overlay or text on the image side for balance */}
                <div className="z-10 text-center p-10">
                    {/* Text is removed as requested */}
                </div>
            </div>
        </div>
    );
};
export default page;