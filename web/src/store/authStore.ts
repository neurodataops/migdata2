import { create } from 'zustand'

interface AuthState {
  token: string | null
  username: string | null
  login: (token: string, username: string) => void
  logout: () => void
}

export const useAuthStore = create<AuthState>((set) => ({
  token: localStorage.getItem('migdata_token'),
  username: localStorage.getItem('migdata_username'),
  login: (token, username) => {
    localStorage.setItem('migdata_token', token)
    localStorage.setItem('migdata_username', username)
    set({ token, username })
  },
  logout: () => {
    localStorage.removeItem('migdata_token')
    localStorage.removeItem('migdata_username')
    set({ token: null, username: null })
  },
}))
