import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable } from 'rxjs';

export interface User {
  id: number;
  username: string;
  password: string;
}

export interface AuthUser {
  id: number;
  username: string;
}

@Injectable({
  providedIn: 'root'
})
export class UserService {
  private currentUserSubject = new BehaviorSubject<AuthUser | null>(null);
  public currentUser$ = this.currentUserSubject.asObservable();

  private users: User[] = [
    { id: 1, username: 'admin', password: 'admin123' },
    { id: 2, username: 'user', password: 'user123' }
  ];

  constructor() {
    // Проверяем localStorage при инициализации
    const savedUser = localStorage.getItem('currentUser');
    if (savedUser) {
      this.currentUserSubject.next(JSON.parse(savedUser));
    }
  }

  login(username: string, password: string): { success: boolean; message?: string; user?: AuthUser } {
    const user = this.users.find(u => u.username === username && u.password === password);
    
    if (user) {
      const authUser: AuthUser = { id: user.id, username: user.username };
      this.currentUserSubject.next(authUser);
      localStorage.setItem('currentUser', JSON.stringify(authUser));
      return { success: true, user: authUser };
    } else {
      return { success: false, message: 'Неверный логин или пароль' };
    }
  }

  register(username: string, password: string, confirmPassword: string): { success: boolean; message?: string } {
    // Проверяем совпадение паролей
    if (password !== confirmPassword) {
      return { success: false, message: 'Пароли не совпадают' };
    }

    // Проверяем минимальную длину пароля
    if (password.length < 6) {
      return { success: false, message: 'Пароль должен содержать минимум 6 символов' };
    }

    // Проверяем существование пользователя
    const existingUser = this.users.find(u => u.username === username);
    if (existingUser) {
      return { success: false, message: 'Пользователь с таким именем уже существует' };
    }

    // Регистрируем нового пользователя
    const newUser: User = {
      id: Math.max(...this.users.map(u => u.id)) + 1,
      username,
      password
    };
    this.users.push(newUser);

    // Автоматически авторизуем пользователя после регистрации
    const authUser: AuthUser = { id: newUser.id, username: newUser.username };
    this.currentUserSubject.next(authUser);
    localStorage.setItem('currentUser', JSON.stringify(authUser));

    return { success: true };
  }

  logout(): void {
    this.currentUserSubject.next(null);
    localStorage.removeItem('currentUser');
  }

  getCurrentUser(): AuthUser | null {
    return this.currentUserSubject.value;
  }

  isLoggedIn(): boolean {
    return this.currentUserSubject.value !== null;
  }
}